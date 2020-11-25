package ddstats

import (
	"time"

	"github.com/jmizell/ddstats/client"
)

type metric struct {
	name  string
	class string
	value float64
	tags  []string
}

func (m *metric) update(v float64) {
	switch m.class {
	case client.Gauge:
		m.value = v
	case client.Count, client.Rate:
		m.value += v
	}
}

func (m *metric) getMetric(namespace, host string, tags []string, interval time.Duration) *client.DDMetric {
	metric := &client.DDMetric{
		Host:   host,
		Metric: prependNamespace(namespace, m.name),
		Tags:   combineTags(m.tags, tags),
		Type:   m.class,
	}
	switch m.class {
	case client.Gauge:
		metric.Points = [][2]interface{}{{time.Now().Unix(), m.value}}
	case client.Rate:
		metric.Interval = int64(interval.Seconds())
		if metric.Interval == 0 {
			metric.Interval = 1
		}
		metric.Points = [][2]interface{}{{time.Now().Unix(), m.value / float64(metric.Interval)}}
	case client.Count:
		metric.Interval = int64(interval.Seconds())
		if metric.Interval == 0 {
			metric.Interval = 1
		}
		metric.Points = [][2]interface{}{{time.Now().Unix(), m.value}}
	}
	return metric
}
