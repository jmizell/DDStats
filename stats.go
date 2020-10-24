package ddstats

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jmizell/ddstats/client"
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
	client        client.APIClient
	metrics       []map[string]*metric
	metricUpdates chan *metric
	workers       []chan *job
	shutdown      chan bool
	flush         chan bool
	workerWG      *sync.WaitGroup
	flushWG       *sync.WaitGroup
	ready         chan bool
	flushCallback func(metrics map[string]*metric)
	errorCallback func(err error, metricSeries []*client.DDMetric)
	errors        []error
	maxErrors     int
	errorLock     *sync.RWMutex
}

func NewStats(cfg *Config) *Stats {

	s := &Stats{
		namespace:     cfg.Namespace,
		host:          cfg.Host,
		tags:          cfg.Tags,
		flushInterval: time.Duration(cfg.FlushIntervalSeconds) * time.Second,
		workerCount:   cfg.WorkerCount,
		workerBuffer:  cfg.WorkerBuffer,
		metricBuffer:  cfg.MetricBuffer,
		maxErrors:     cfg.MaxErrors,
		ready:         make(chan bool, 1),
	}

	if cfg.client != nil {
		s.client = cfg.client
	} else if cfg.APIKey != "" {
		s.client = client.NewDDClient(cfg.APIKey)
	}

	go s.start()
	s.blockReady()
	return s
}

func (c *Stats) start() {

	// Setup our channels
	c.shutdown = make(chan bool)
	c.metricUpdates = make(chan *metric, c.metricBuffer)

	// Setup wait group for workers. Flush wait group is separate as
	// we don't want to block processing new stats, if a flush worker
	// is running slow.
	c.workerWG = &sync.WaitGroup{}
	c.flushWG = &sync.WaitGroup{}

	// Here we're tracking our errors
	c.errors = []error{}
	c.errorLock = &sync.RWMutex{}

	// Setup our slice of map metrics. There is a separate map for each worker
	// so we can avoid locking on storing metrics. This will be zeroed out at
	// each flush cycle.
	c.metrics = make([]map[string]*metric, c.workerCount)
	for i := range c.metrics {
		c.metrics[i] = map[string]*metric{}
	}

	// Start our works, each worker has it's own channel.
	c.workers = make([]chan *job, c.workerCount)
	for i := 0; i < c.workerCount; i++ {
		c.workers[i] = make(chan *job, c.workerBuffer)
		go c.worker(c.workers[i], i)
	}

	// Start the flush worker. This will send a flush signal until given
	// a shutdown signal.
	cancelFlush := make(chan bool)
	c.flush = make(chan bool)
	go func() {
		flush := time.NewTicker(c.flushInterval)
		for {
			select {
			case <-flush.C:
				c.flush <- true
			case <-cancelFlush:
				flush.Stop()
				c.workerWG.Done()
				return
			}
		}
	}()
	c.ready <- true

	// We need to track time between flushes. If a flush is called before the scheduled
	// interval, we will need to know exactly how much time has passed, so we can calculate
	// our rate metrics.
	lastFlush := time.Now()
	for {
		select {
		case metric := <-c.metricUpdates:
			// New metric has been sent, we want to add a job to the wait group, and
			// then we assign it to the worker by using a FNV-1a hash. This should ensure
			// that the same worker always sees the same metric.
			c.workerWG.Add(1)
			c.workers[fnv1a(metric.name)%uint32(len(c.workers))] <- &job{metric: metric}
		case <-c.flush:

			// TODO make non-blocking

			// On a flush signal we need to wait for all current metrics to be processed
			// by the workers
			c.workerWG.Wait()

			// We need to make a copy of all the metrics to a new data structure
			flattenedMetrics := make(map[string]*metric)
			for _, m := range c.metrics {
				for k, v := range m {
					flattenedMetrics[k] = v
				}
			}

			// Then we zero out all of the metrics, and start with new values for the
			// next flush interval.
			for i := range c.metrics {
				c.metrics[i] = map[string]*metric{}
			}

			// Update the flush interval, add a job to the flush wait group, and send
			// the metrics to the flush worker.
			interval := time.Since(lastFlush)
			c.flushWG.Add(1)
			go c.send(flattenedMetrics, interval)
			lastFlush = time.Now()
		case <-c.shutdown:

			// On shutdown, we'll signal all the workers to exit after completing the current job
			for i := range c.workers {
				c.workerWG.Add(1)
				c.workers[i] <- &job{shutdown: true}
			}

			// Signal to the flush worker to shutdown
			c.workerWG.Add(1)
			cancelFlush <- true

			// Finally wait for the workers to shutdown before returning
			c.workerWG.Wait()
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
			c.workerWG.Done()
			return
		}

		// Metrics are indexed by a combination of the metric name, and the list
		// of tags. Order of the tags sent to the job shouldn't matter, as we
		// sort them, before creating the index key.
		key := metricKey(job.metric.name, job.metric.tags)

		// Store or update the metric
		if _, ok := c.metrics[id][key]; ok {
			c.metrics[id][key].update(job.metric.value)
		} else {
			c.metrics[id][key] = job.metric
		}

		// Signalling done, allows us to track if any jobs are being worked on,
		// in order for us to avoid concurrent access to the metrics map on a
		// flush operation.
		c.workerWG.Done()
	}
}

func (c *Stats) send(metrics map[string]*metric, flushTime time.Duration) {

	defer c.flushWG.Done()

	if len(metrics) == 0 {
		return
	}

	metricsSeries := make([]*client.DDMetric, 0, len(metrics))
	for _, m := range metrics {
		metricsSeries = append(metricsSeries, m.getMetric(c.namespace, c.host, c.tags, flushTime))
	}
	if err := c.SendSeries(metricsSeries); err != nil {
		c.errorLock.Lock()
		c.errors = appendErrorsList(c.errors, err, c.maxErrors)
		c.errorLock.Unlock()
		if c.errorCallback != nil {
			c.errorCallback(err, metricsSeries)
		}
	}

	if c.flushCallback != nil {
		c.flushCallback(metrics)
	}
}

// SendSeries immediately posts an DDMetric series to the Datadog api. Each metric in the series
// is checked for an host name, and the correct namespace. If host, or namespace vales are missing,
// the values will be filled before sending to the api. Global tags are added to all metrics.
func (c *Stats) SendSeries(series []*client.DDMetric) error {
	for _, m := range series {
		if m.Host == "" {
			m.Host = c.host
		}
		m.Metric = prependNamespace(c.namespace, m.Metric)
		m.Tags = combineTags(c.tags, m.Tags)
	}
	return c.client.SendSeries(&client.DDMetricSeries{Series: series})
}

// ServiceCheck immediately posts an DDServiceCheck to he Datadog api. The namespace is
// prepended to the check name, if it is missing. Host, and time is automatically added.
// Global tags are appended to tags passed to the method.
func (c *Stats) ServiceCheck(check, message string, status client.Status, tags []string) error {
	return c.client.SendServiceCheck(&client.DDServiceCheck{
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
func (c *Stats) Event(event *client.DDEvent) error {
	if event.Host == "" {
		event.Host = c.host
	}
	event.AggregationKey = prependNamespace(c.namespace, event.AggregationKey)
	event.Tags = combineTags(c.tags, event.Tags)
	return c.client.SendEvent(event)
}

// Increment creates or increments a count metric by +1. This is a non-blocking method, if
// the channel buffer is full, then the metric is not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Increment(name string, tags []string) {
	c.Count(name, 1, tags)
}

// Decrement creates or subtracts a count metric by -1. This is a non-blocking method, if
// the channel buffer is full, then the metric is not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Decrement(name string, tags []string) {
	c.Count(name, -1, tags)
}

// Count creates or adds a count metric by value. This is a non-blocking method, if
// the channel buffer is full, then the metric is not recorded. Count stats are sent as rate,
// by taking the count value and dividing by the number of seconds since the last flush.
func (c *Stats) Count(name string, value float64, tags []string) {
	select {
	case c.metricUpdates <- &metric{
		name:  name,
		class: client.Count,
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
		class: client.Gauge,
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
func (c *Stats) ErrorCallback(f func(err error, metricSeries []*client.DDMetric)) {
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
	c.workerWG.Wait()
	c.flushWG.Wait()
}

func prependNamespace(namespace, name string) string {

	if namespace == "" || strings.HasPrefix(name, namespace) {
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

	// Tags should be unique, duplicate tags should be filtered
	// out of the list
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

	// We have to sort the tags, in order to generate a consistent key.
	// If a user swaps the order of a key, we don't want to store that
	// metric as new metric.
	sort.Strings(tags)
	return fmt.Sprintf("%s%s", name, strings.Join(tags, ""))
}

func fnv1a(v string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(v))
	return h.Sum32()
}

func appendErrorsList(errors []error, err error, max int) []error {

	if len(errors) >= max {
		errors = errors[1:]
	}

	return append(errors, err)
}
