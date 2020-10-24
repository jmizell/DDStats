package client

// DDMetric Type values
const (
	Count = "count"
	Gauge = "gauge"
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
