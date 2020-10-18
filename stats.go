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
	DefaultWorkerCount  = 10
	DefaultWorkerBuffer = 100
)

type job struct {
	metric   *metric
	shutdown bool
}

type Stats struct {
	namespace     string
	tags          []string
	workerCount   int
	workerBuffer  int
	metricBuffer  int
	client        *Client
	metrics       []map[string]*metric
	metricUpdates chan *metric
	workers       []chan *job
	shutdown      chan bool
	flush         chan bool
	wg            *sync.WaitGroup
	flushCallback func(metrics map[string]*metric)
	errorCallback func(err error)
	errors        []error
	errorLock     *sync.RWMutex
}

func NewStats(namespace string, tags []string) *Stats {
	s := &Stats{
		namespace:    namespace,
		tags:         tags,
		workerCount:  DefaultWorkerCount,
		workerBuffer: DefaultWorkerBuffer,
		metricBuffer: DefaultWorkerBuffer * DefaultWorkerCount,
		client:       NewClient(),
	}
	s.start()
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

	cancelFlush := make(chan bool)
	go func() {
		flush := time.NewTicker(time.Minute * 1)
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

	for {
		select {
		case metric := <-c.metricUpdates:
			c.wg.Add(1)
			c.workers[fnv1a(metric.name)%uint32(len(c.workers))] <- &job{metric: metric}
		case <-c.flush:
			c.wg.Wait()
			metrics := c.metrics
			c.metrics = make([]map[string]*metric, c.workerCount)
			for i := range c.metrics {
				c.metrics[i] = map[string]*metric{}
			}
			go c.send(metrics)
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

func (c *Stats) send(metrics []map[string]*metric) {

	flattenedMetrics := make(map[string]*metric)
	for _, m := range metrics {
		for k, v := range m {
			flattenedMetrics[k] = v
		}
	}

	if len(flattenedMetrics) == 0 {
		return
	}

	metricsSeries := make(DDMetricSeries, 0, len(flattenedMetrics))
	for _, m := range flattenedMetrics {
		metricsSeries = append(metricsSeries, m.getMetric(c.namespace, c.tags))
	}
	if err := c.SendSeries(metricsSeries); err != nil {
		c.errorLock.Lock()
		c.errors = append(c.errors, err)
		c.errorLock.Unlock()
		if c.errorCallback != nil {
			c.errorCallback(err)
		}
	}

	if c.flushCallback != nil {
		c.flushCallback(flattenedMetrics)
	}
}

func (c *Stats) SendSeries(series DDMetricSeries) error {
	return c.client.SendSeries(series)
}

func (c *Stats) ServiceCheck(check, host, message string, status Status, tags []string) error {
	return c.client.SendServiceCheck(&DDServiceCheck{
		Check:     check,
		Hostname:  host,
		Message:   message,
		Status:    status,
		Tags:      tags,
		Timestamp: time.Now().Unix(),
	})
}

func (c *Stats) Event(event *DDEvent) error {
	return c.client.SendEvent(event)
}

func (c *Stats) Increment(name string, tags []string) {
	c.Count(name, 1, tags)
}

func (c *Stats) Decrement(name string, tags []string) {
	c.Count(name, -1, tags)
}

func (c *Stats) Count(name string, value int64, tags []string) {

	c.metricUpdates <- &metric{name: name, class: metricCount, value: value, tags: tags}
}

func (c *Stats) Gauge(name string, value float64, tags []string) {
	c.metricUpdates <- &metric{name: name, class: metricGauge, value: value, tags: tags}
}

func (c *Stats) Flush() {
	c.flush <- true
}

func (c *Stats) FlushCallback(f func(metrics map[string]*metric)) {
	c.flushCallback = f
}

func (c *Stats) ErrorCallback(f func(err error)) {
	c.errorCallback = f
}

func (c *Stats) Errors() []error {
	c.errorLock.RLock()
	defer c.errorLock.RUnlock()
	errs := c.errors
	return errs
}

func (c *Stats) Close() {
	c.Flush()
	c.shutdown <- true
	c.wg.Wait()
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
