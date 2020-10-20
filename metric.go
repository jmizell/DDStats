package DDStats

import (
	"time"
)

// DDMetric Type values
const (
	metricCount = "count"
	metricGauge = "gauge"
)

type DDMetric struct {
	Host     string           `json:"host"`
	Interval int64            `json:"interval"`
	Metric   string           `json:"metric"`
	Points   [][2]interface{} `json:"points"`
	Tags     []string         `json:"tags"`
	Type     string           `json:"type"`
}

type DDMetricSeries struct {
	Series []*DDMetric `json:"series"`
}

type metric struct {
	name  string
	class string
	value float64
	tags  []string
}

func (m *metric) update(v float64) {
	switch m.class {
	case metricGauge:
		m.value = v
	case metricCount:
		m.value += v
	}
}

func (m *metric) getMetric(namespace, host string, tags []string, interval time.Duration) *DDMetric {
	metric := &DDMetric{
		Host:   host,
		Metric: prependNamespace(namespace, m.name),
		Tags:   combineTags(m.tags, tags),
		Type:   m.class,
	}
	switch m.class {
	case metricGauge:
		metric.Points = [][2]interface{}{{time.Now().Unix(), m.value}}
	case metricCount:
		metric.Interval = int64(interval.Seconds())
		if metric.Interval == 0 {
			metric.Interval = 1
		}
		metric.Points = [][2]interface{}{{time.Now().Unix(), m.value / float64(metric.Interval)}}
	}
	return metric
}
