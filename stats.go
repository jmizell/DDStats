package DDStats

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultWorkerCount   = 10
	DefaultWorkerBuffer  = 100
	DefaultFlushInterval = time.Second * 60
)

type job struct {
	metric   *metric
	shutdown bool
}

type Stats struct {
	namespace     string
	host          string
	tags          []string
	flushInterval time.Duration
	workerCount   int
	workerBuffer  int
	metricBuffer  int
	client        APIClient
	metrics       []map[string]*metric
	metricUpdates chan *metric
	workers       []chan *job
	shutdown      chan bool
	flush         chan bool
	wg            *sync.WaitGroup
	flushWG       *sync.WaitGroup
	ready         chan bool
	flushCallback func(metrics map[string]*metric)
	errorCallback func(err error)
	errors        []error
	errorLock     *sync.RWMutex
}

func NewStats(namespace, host, apiKey string, tags []string) *Stats {
	// TODO implement config options
	s := &Stats{
		namespace:     namespace,
		host:          host,
		tags:          tags,
		flushInterval: DefaultFlushInterval,
		workerCount:   DefaultWorkerCount,
		workerBuffer:  DefaultWorkerBuffer,
		metricBuffer:  DefaultWorkerBuffer * DefaultWorkerCount,
		ready:         make(chan bool, 1),
		client:        NewDDClient(apiKey),
	}
	go s.start()
	s.blockReady()
	return s
}

func (c *Stats) start() {

	c.errors = []error{}
	c.errorLock = &sync.RWMutex{}
	c.metrics = make([]map[string]*metric, c.workerCount)
	for i := range c.metrics {
		c.metrics[i] = map[string]*metric{}
	}
	c.metricUpdates = make(chan *metric, c.metricBuffer)
	c.workers = make([]chan *job, c.workerCount)
	for i := 0; i < c.workerCount; i++ {
		c.workers[i] = make(chan *job, c.workerBuffer)
		go c.worker(c.workers[i], i)
	}
	c.shutdown = make(chan bool)
	c.flush = make(chan bool)
	c.wg = &sync.WaitGroup{}
	c.flushWG = &sync.WaitGroup{}

	cancelFlush := make(chan bool)
	go func() {
		flush := time.NewTicker(c.flushInterval)
		for {
			select {
			case <-flush.C:
				c.flush <- true
			case <-cancelFlush:
				flush.Stop()
				c.wg.Done()
				return
			}
		}
	}()
	c.ready <- true

	lastFlush := time.Now()
	for {
		select {
		case metric := <-c.metricUpdates:
			c.wg.Add(1)
			c.workers[fnv1a(metric.name)%uint32(len(c.workers))] <- &job{metric: metric}
		case <-c.flush:
			// TODO make non-blocking
			c.wg.Wait()
			flattenedMetrics := make(map[string]*metric)
			for _, m := range c.metrics {
				for k, v := range m {
					flattenedMetrics[k] = v
				}
			}
			for i := range c.metrics {
				c.metrics[i] = map[string]*metric{}
			}
			interval := time.Since(lastFlush)
			c.flushWG.Add(1)
			go c.send(flattenedMetrics, interval)
			lastFlush = time.Now()
		case <-c.shutdown:
			for i := range c.workers {
				c.wg.Add(1)
				c.workers[i] <- &job{shutdown: true}
			}
			c.wg.Add(1)
			cancelFlush <- true
			c.wg.Wait()
			return
		}
	}
}

func (c *Stats) blockReady() {
	<-c.ready
}

func (c *Stats) worker(jobs chan *job, id int) {
	for {
		job := <-jobs
		if job.shutdown {
			c.wg.Done()
			return
		}

		key := metricKey(job.metric.name, job.metric.tags)
		if _, ok := c.metrics[id][key]; ok {
			c.metrics[id][key].update(job.metric.value)
		} else {
			c.metrics[id][key] = job.metric
		}
		c.wg.Done()
	}
}

func (c *Stats) send(metrics map[string]*metric, flushTime time.Duration) {

	defer c.flushWG.Done()

	if len(metrics) == 0 {
		return
	}

	metricsSeries := make([]*DDMetric, 0, len(metrics))
	for _, m := range metrics {
		metricsSeries = append(metricsSeries, m.getMetric(c.namespace, c.host, c.tags, flushTime))
	}
	if err := c.SendSeries(metricsSeries); err != nil {
		c.errorLock.Lock()
		// TODO keep this list from growing large, cap size
		c.errors = append(c.errors, err)
		c.errorLock.Unlock()
		if c.errorCallback != nil {
			c.errorCallback(err)
		}
	}

	if c.flushCallback != nil {
		c.flushCallback(metrics)
	}
}

// SendSeries immediately posts an DDMetric series to the Datadog api. Each metric in the series
// is checked for an host name, and the correct namespace. If host, or namespace vales are missing,
// the values will be filled before sending to the api. Global tags are added to all metrics.
func (c *Stats) SendSeries(series []*DDMetric) error {
	for _, m := range series {
		if m.Host == "" {
			m.Host = c.host
		}
		m.Metric = prependNamespace(c.namespace, m.Metric)
		m.Tags = combineTags(c.tags, m.Tags)
	}
	return c.client.SendSeries(&DDMetricSeries{Series: series})
}

// ServiceCheck immediately posts an DDServiceCheck to he Datadog api. The namespace is
// prepended to the check name, if it is missing. Host, and time is automatically added.
// Global tags are appended to tags passed to the method.
func (c *Stats) ServiceCheck(check, message string, status Status, tags []string) error {
	return c.client.SendServiceCheck(&DDServiceCheck{
		Check:     prependNamespace(c.namespace, check),
		Hostname:  c.host,
		Message:   message,
		Status:    status,
		Tags:      combineTags(c.tags, tags),
		Timestamp: time.Now().Unix(),
	})
}

// Event immediately posts an DDEvent to he Datadog api. If host, or namespace vales are missing,
// the values will be filled before sending to the api. Global tags are appended to the event.
func (c *Stats) Event(event *DDEvent) error {
	if event.Host == "" {
		event.Host = c.host
	}
	event.AggregationKey = prependNamespace(c.namespace, event.AggregationKey)
	event.Tags = combineTags(c.tags, event.Tags)
	return c.client.SendEvent(event)
}

// Increment creates or increments a count metric by +1. This is a non-blocking method, if
// the channel buffer is full, then the metric not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Increment(name string, tags []string) {
	c.Count(name, 1, tags)
}

// Decrement creates or subtracts a count metric by -1. This is a non-blocking method, if
// the channel buffer is full, then the metric not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Decrement(name string, tags []string) {
	c.Count(name, -1, tags)
}

// Count creates or adds a count metric by value. This is a non-blocking method, if
// the channel buffer is full, then the metric not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Count(name string, value float64, tags []string) {
	select {
	case c.metricUpdates <- &metric{
		name:  name,
		class: metricCount,
		value: value,
		tags:  tags,
	}:
	default:
	}
}

// Gauge creates or updates a gauge metric by value. This is a non-blocking method, if
// the channel buffer is full, then the metric not recorded. Gauge stats are reported
// as the last value sent before flush is called.
func (c *Stats) Gauge(name string, value float64, tags []string) {
	select {
	case c.metricUpdates <- &metric{
		name:  name,
		class: metricGauge,
		value: value,
		tags:  tags,
	}:
	default:
	}
}

// Flush signals the main worker thread to copy all current metrics, and send them
// to the Datadog api.
func (c *Stats) Flush() {
	c.flush <- true
}

// FlushCallback registers a call back function that will be called at the end of every successful flush.
func (c *Stats) FlushCallback(f func(metrics map[string]*metric)) {
	c.flushCallback = f
}

// ErrorCallback registers a call back function that will be called if any error is returned
// by the api client during a flush.
func (c *Stats) ErrorCallback(f func(err error)) {
	c.errorCallback = f
}

// Errors returns a slice of all errors returned by the api client during a flush.
func (c *Stats) Errors() []error {
	c.errorLock.RLock()
	defer c.errorLock.RUnlock()
	errs := c.errors
	return errs
}

// Close signals a shutdown, and blocks while waiting for a final flush, and all workers to shutdown.
func (c *Stats) Close() {
	c.Flush()
	// TODO this will block indefinitely if called twice
	c.shutdown <- true
	c.wg.Wait()
	c.flushWG.Wait()
}

func prependNamespace(namespace, name string) string {

	if namespace == "" ||  strings.HasPrefix(name, namespace) {
		return name
	}

	return fmt.Sprintf("%s.%s", namespace, name)
}

func combineTags(tags1, tags2 []string) []string {

	if tags1 == nil && tags2 == nil {
		return []string{}
	} else if tags1 == nil {
		return tags2
	} else if tags2 == nil {
		return tags1
	}

	uniqueTags := make(map[string]bool)
	for _, tag := range append(tags1, tags2...) {
		uniqueTags[tag] = true
	}

	newTags := make([]string, 0, len(uniqueTags))
	for tag := range uniqueTags {
		newTags = append(newTags, tag)
	}

	return newTags
}

func metricKey(name string, tags []string) string {
	sort.Strings(tags)
	return fmt.Sprintf("%s%s", name, strings.Join(tags, ""))
}

func fnv1a(v string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(v))
	return h.Sum32()
}
