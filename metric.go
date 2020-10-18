package DDStats

const (
	metricCount = "count"
	metricGauge = "gauge"
)

type DDMetric struct {
	Host     string       `json:"host"`
	Interval int64        `json:"interval"`
	Metric   string       `json:"metric"`
	Points   [][2]float64 `json:"points"`
	Tags     []string     `json:"tags"`
	Type     string       `json:"type"`
}

type DDMetricSeries []*DDMetric

type metric struct {
	name  string
	class string
	value interface{}
	tags  []string
}

func (m *metric) update(v interface{}) {
	switch m.class {
	case metricCount:
		x := m.value.(int64)
		x += v.(int64)
		m.value = x
	case metricGauge:
		m.value = v
	}
}

func (m *metric) getMetric(namespace string, tags []string) *DDMetric {
	panic("implement me")
}