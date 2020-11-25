package ddstats

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmizell/ddstats/client"
)

type TestAPIClient struct {
	series []*client.DDMetricSeries
	checks []*client.DDServiceCheck
	events []*client.DDEvent
	lock   *sync.Mutex

	sendEventError  error
	sendSeriesError error
}

func NewTestAPIClient() *TestAPIClient {
	return &TestAPIClient{
		series: []*client.DDMetricSeries{},
		checks: []*client.DDServiceCheck{},
		events: []*client.DDEvent{},
		lock:   &sync.Mutex{},
	}
}

func (t *TestAPIClient) SendSeries(series *client.DDMetricSeries) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.series = append(t.series, series)
	return t.sendSeriesError
}

func (t *TestAPIClient) SendServiceCheck(check *client.DDServiceCheck) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.checks = append(t.checks, check)
	return nil
}

func (t *TestAPIClient) SendEvent(event *client.DDEvent) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.events = append(t.events, event)
	return t.sendEventError
}

func (t *TestAPIClient) SetHTTPClient(client.HTTPClient) {}

func (t *TestAPIClient) FindMetric(callIndex int, ddMetric *client.DDMetric) error {

	if callIndex > len(t.series)-1 {
		return fmt.Errorf("")
	}

	findKey := metricKey(ddMetric.Metric, ddMetric.Tags)
	for _, m := range t.series[callIndex].Series {
		key := metricKey(m.Metric, m.Tags)
		if key == findKey {

			if m.Host != ddMetric.Host {
				return fmt.Errorf("for metric %s, expected host %s, have %s", key, ddMetric.Host, m.Host)
			}

			if m.Metric != ddMetric.Metric {
				return fmt.Errorf("for metric %s, expected metric name %s, have %s", key, ddMetric.Metric, m.Metric)
			}

			if m.Interval != ddMetric.Interval {
				return fmt.Errorf("for metric %s, expected interval to be %d, have %d", key, ddMetric.Interval, m.Interval)
			}

			if m.Type != ddMetric.Type {
				return fmt.Errorf("for metric %s, expected type to be %s, have %s", key, ddMetric.Type, m.Type)
			}

			if len(m.Points) != len(ddMetric.Points) {
				return fmt.Errorf("for metric %s, expected type to have %d points, have %d", key, len(ddMetric.Points), len(m.Points))
			}

			for i, p := range m.Points {
				if p[1] != ddMetric.Points[i][1] {
					return fmt.Errorf("for metric %s, expected point %d to have %v points, have %v", key, i, ddMetric.Points[i][1], p[1])
				}
			}

			return nil
		}
	}

	return fmt.Errorf("could not find metric in calls")
}

func (t *TestAPIClient) TestValidateCalls(seriesCalls []*client.DDMetricSeries, checkCalls, eventCalls int) error {

	if len(t.series) != len(seriesCalls) {
		return fmt.Errorf("expected %d calls to SendSeries, have %d", len(seriesCalls), len(t.series))
	}

	for i, call := range seriesCalls {
		for _, metric := range call.Series {
			if err := t.FindMetric(i, metric); err != nil {
				return err
			}
		}
	}

	if len(t.checks) != checkCalls {
		return fmt.Errorf("expected %d calls to SendServiceCheck, have %d", checkCalls, len(t.checks))
	}

	if len(t.events) != eventCalls {
		return fmt.Errorf("expected %d calls to SendServiceCheck, have %d", eventCalls, len(t.events))
	}

	return nil
}

const (
	testNamespace = "testNamespace"
	testHost      = "testHost"
)

var testTags = []string{"tag:1"}

func NewTestStats() (*Stats, *TestAPIClient, error) {

	testClient := NewTestAPIClient()

	cfg := NewConfig().
		WithNamespace(testNamespace).
		WithHost(testHost).
		WithTags(testTags).
		WithClient(testClient)
	cfg.WorkerBuffer = 10
	cfg.WorkerCount = 2
	cfg.MetricBuffer = 10 * 2
	cfg.FlushIntervalSeconds = 60

	stat, err := NewStats(cfg)

	return stat, testClient, err
}

func NewTestStatsWithStart() (*Stats, *TestAPIClient, error) {
	stats, testApi, err := NewTestStats()
	if err != nil {
		return nil, nil, err
	}
	go stats.start()
	stats.blockReady()
	return stats, testApi, err
}

func TestNewStats(t *testing.T) {
	t.Run("no client", func(tt *testing.T) {
		stat, err := NewStats(NewConfig())
		if err == nil {
			tt.Fatalf("expected error with no client")
		}
		if stat != nil {
			tt.Fatalf("expected stat to be nil")
		}
	})
	t.Run("with api key", func(tt *testing.T) {
		stat, err := NewStats(NewConfig().WithAPIKey("key"))
		if err != nil {
			tt.Fatalf("expected no error have %s", err.Error())
		}
		if stat == nil {
			tt.Fatalf("expected stat to be not nil")
		}
	})
}

func TestStats_SendSeries(t *testing.T) {

	t.Run("one metric", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		metrics := []*client.DDMetric{
			{
				Host:     "testHost2",
				Interval: 10,
				Metric:   "metric1",
				Points:   [][2]interface{}{{1, 2}},
				Tags:     []string{"tag:2"},
				Type:     client.Count,
			},
		}

		if err := stats.SendSeries(metrics); err != nil {
			tt.Fatalf("send returned err, %s", err.Error())
		}

		seriesCalls := []*client.DDMetricSeries{
			{
				Series: []*client.DDMetric{
					{
						Host:     "testHost2",
						Metric:   "testNamespace.metric1",
						Tags:     []string{"tag:2", "tag:1"},
						Interval: 10,
						Type:     client.Count,
						Points:   [][2]interface{}{{1, 2}},
					},
				},
			},
		}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one metric no host", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		metrics := []*client.DDMetric{
			{
				Interval: 10,
				Metric:   "metric1",
				Points:   [][2]interface{}{{1, 2}},
				Tags:     []string{"tag:2"},
				Type:     client.Count,
			},
		}

		if err := stats.SendSeries(metrics); err != nil {
			tt.Fatalf("send returned err, %s", err.Error())
		}

		seriesCalls := []*client.DDMetricSeries{
			{
				Series: []*client.DDMetric{
					{
						Host:     testHost,
						Metric:   "testNamespace.metric1",
						Tags:     []string{"tag:2", "tag:1"},
						Interval: 10,
						Type:     client.Count,
						Points:   [][2]interface{}{{1, 2}},
					},
				},
			},
		}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})
}

func TestStats_QueueSeries(t *testing.T) {

	t.Run("one metric", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		metrics := []*client.DDMetric{
			{
				Host:     "testHost2",
				Interval: 10,
				Metric:   "metric1",
				Points:   [][2]interface{}{{1, 2}},
				Tags:     []string{"tag:2"},
				Type:     client.Count,
			},
		}

		stats.QueueSeries(metrics)
		stats.Close()

		seriesCalls := []*client.DDMetricSeries{
			{
				Series: []*client.DDMetric{
					{
						Host:     "testHost2",
						Metric:   "testNamespace.metric1",
						Tags:     []string{"tag:2", "tag:1"},
						Interval: 10,
						Type:     client.Count,
						Points:   [][2]interface{}{{1, 2}},
					},
				},
			},
		}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one metric no host", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		metrics := []*client.DDMetric{
			{
				Interval: 10,
				Metric:   "metric1",
				Points:   [][2]interface{}{{1, 2}},
				Tags:     []string{"tag:2"},
				Type:     client.Count,
			},
		}

		stats.QueueSeries(metrics)
		stats.Close()

		seriesCalls := []*client.DDMetricSeries{
			{
				Series: []*client.DDMetric{
					{
						Host:     testHost,
						Metric:   "testNamespace.metric1",
						Tags:     []string{"tag:2", "tag:1"},
						Interval: 10,
						Type:     client.Count,
						Points:   [][2]interface{}{{1, 2}},
					},
				},
			},
		}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})
}

func TestStats_Close(t *testing.T) {

	baseMetric := client.DDMetric{
		Host:     testHost,
		Metric:   "testNamespace.test",
		Tags:     []string{"tag:1"},
		Interval: 1,
		Points:   [][2]interface{}{{1, float64(1)}},
		Type:     client.Rate,
	}
	seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&baseMetric}}}

	t.Run("one call to close", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two calls to close", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()
		stats.Close()

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("many calls close", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < 50; i++ {
			go stats.Close()
		}
		time.Sleep(time.Second * 1)

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})
}

func TestStats_Flush(t *testing.T) {
	baseMetric := client.DDMetric{
		Host:     testHost,
		Metric:   "testNamespace.test",
		Tags:     []string{"tag:1"},
		Interval: 1,
		Points:   [][2]interface{}{{1, float64(1)}},
		Type:     client.Rate,
	}
	seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&baseMetric}}}

	stats, testApi, err := NewTestStatsWithStart()
	if err != nil {
		t.Fatalf(err.Error())
	}
	stats.IncrementRate("test", nil)
	time.Sleep(time.Millisecond * 100)
	stats.Flush()

	if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
		t.Fatalf(err.Error())
	}
}

func TestStats_CountGaugeRate(t *testing.T) {

	baseMetric := client.DDMetric{
		Host:     testHost,
		Metric:   "testNamespace.test",
		Tags:     []string{"tag:1"},
		Interval: 1,
		Type:     client.Rate,
	}

	t.Run("one increment rate", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(1)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two increment rate", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		stats.IncrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one decrement rate", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.DecrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-1)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two decrement rate", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.DecrementRate("test", nil)
		stats.DecrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-2)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("three increment rate one decrement rate", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.IncrementRate("test", nil)
		stats.IncrementRate("test", nil)
		stats.IncrementRate("test", nil)
		stats.DecrementRate("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("rate value 10", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Rate("test", 10, nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one increment count", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Increment("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(1)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two increment count", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one decrement count", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-1)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two decrement count", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Decrement("test", nil)
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-2)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("three increment count one decrement count", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("count value 10", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Count("test", 10, nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		m1.Type = client.Count
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one gauge", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		stats.Gauge("test", 10, nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		m1.Type = client.Gauge
		m1.Interval = 0
		seriesCalls := []*client.DDMetricSeries{{Series: []*client.DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("no calls", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		if err := testApi.TestValidateCalls([]*client.DDMetricSeries{}, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})
}

func TestStats_Event(t *testing.T) {

	testEvent := client.DDEvent{
		AggregationKey: "testEvent",
		AlertType:      client.AlertError,
		DateHappened:   time.Now().Unix(),
		DeviceName:     "device1",
		Host:           "host1",
		Priority:       client.PriorityNormal,
		SourceTypeName: "",
		Tags:           nil,
		Title:          "",
	}

	t.Run("no error", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		sendEvent := testEvent
		if err := stats.Event(&sendEvent); err != nil {
			tt.Fatalf("expected no error on send, have %s", err.Error())
		}

		if len(testApi.events) != 1 {
			tt.Fatalf("expected %d calls to send events, have %d", 1, len(testApi.events))
		}

		if len(testApi.events[0].Tags) != 1 {
			tt.Fatalf("expected %d tags, have %d", 1, len(testApi.events[0].Tags))
		}

		if testApi.events[0].Host != testEvent.Host {
			tt.Fatalf("expected host to be %s, have %s", testEvent.Host, testApi.events[0].Host)
		}

		if testApi.events[0].AggregationKey != "testNamespace.testEvent" {
			tt.Fatalf("expected AggregationKey to be %s, have %s", "testNamespace.testEvent", testApi.events[0].AggregationKey)
		}
	})

	t.Run("no host", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		sendEvent := testEvent
		sendEvent.Host = ""
		if err := stats.Event(&sendEvent); err != nil {
			tt.Fatalf("expected no error on send, have %s", err.Error())
		}

		testEventNoHost := testEvent
		testEventNoHost.Host = testHost

		if len(testApi.events) != 1 {
			tt.Fatalf("expected %d calls to send events, have %d", 1, len(testApi.events))
		}

		if len(testApi.events[0].Tags) != 1 {
			tt.Fatalf("expected %d tags, have %d", 1, len(testApi.events[0].Tags))
		}

		if testApi.events[0].Host != testEventNoHost.Host {
			tt.Fatalf("expected host to be %s, have %s", testEventNoHost.Host, testApi.events[0].Host)
		}

		if testApi.events[0].AggregationKey != "testNamespace.testEvent" {
			tt.Fatalf("expected AggregationKey to be %s, have %s", "testNamespace.testEvent", testApi.events[0].AggregationKey)
		}
	})

	t.Run("no date", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		sendEvent := testEvent
		sendEvent.DateHappened = 0
		if err := stats.Event(&sendEvent); err != nil {
			tt.Fatalf("expected no error on send, have %s", err.Error())
		}

		if len(testApi.events) != 1 {
			tt.Fatalf("expected %d calls to send events, have %d", 1, len(testApi.events))
		}

		if len(testApi.events[0].Tags) != 1 {
			tt.Fatalf("expected %d tags, have %d", 1, len(testApi.events[0].Tags))
		}

		if testApi.events[0].Host != testEvent.Host {
			tt.Fatalf("expected host to be %s, have %s", testEvent.Host, testApi.events[0].Host)
		}

		if testApi.events[0].DateHappened == 0 {
			tt.Fatalf("expected DateHappened to be set, have %d", testApi.events[0].DateHappened)
		}

		if testApi.events[0].AggregationKey != "testNamespace.testEvent" {
			tt.Fatalf("expected AggregationKey to be %s, have %s", "testNamespace.testEvent", testApi.events[0].AggregationKey)
		}
	})

	t.Run("error", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		testApi.sendEventError = fmt.Errorf("test error")
		sendEvent := testEvent
		if err := stats.Event(&sendEvent); err == nil {
			tt.Fatalf("expected error on send, have none")
		}

		if len(testApi.events) != 1 {
			tt.Fatalf("expected %d calls to send events, have %d", 1, len(testApi.events))
		}

		if len(testApi.events[0].Tags) != 1 {
			tt.Fatalf("expected %d tags, have %d", 1, len(testApi.events[0].Tags))
		}

		if testApi.events[0].Host != testEvent.Host {
			tt.Fatalf("expected host to be %s, have %s", testEvent.Host, testApi.events[0].Host)
		}

		if testApi.events[0].AggregationKey != "testNamespace.testEvent" {
			tt.Fatalf("expected AggregationKey to be %s, have %s", "testNamespace.testEvent", testApi.events[0].AggregationKey)
		}
	})
}

func TestStats_ServiceCheck(t *testing.T) {
	t.Run("no error", func(tt *testing.T) {
		stats, testApi, err := NewTestStats()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		if err := stats.ServiceCheck("check1", "failed", client.Warning, nil); err != nil {
			tt.Fatalf("expected no error on service check, have %s", err.Error())
		}

		if len(testApi.checks) != 1 {
			tt.Fatalf("expected %d calls to send check, have %d", 1, len(testApi.checks))
		}

		if len(testApi.checks[0].Tags) != 1 {
			tt.Fatalf("expected %d tags, have %d", 1, len(testApi.checks[0].Tags))
		}

		if testApi.checks[0].Hostname != testHost {
			tt.Fatalf("expected host to be %s, have %s", testHost, testApi.checks[0].Hostname)
		}

		if testApi.checks[0].Check != "testNamespace.check1" {
			tt.Fatalf("expected check name to be %s, have %s", "testNamespace.check1", testApi.checks[0].Check)
		}

		if testApi.checks[0].Message != "failed" {
			tt.Fatalf("expected check message to be %s, have %s", "failed", testApi.checks[0].Message)
		}

		if testApi.checks[0].Status != client.Warning {
			tt.Fatalf("expected check message to be %d, have %d", client.Warning, testApi.checks[0].Status)
		}
	})
}

func TestStats_Errors(t *testing.T) {

	t.Run("no error", func(tt *testing.T) {
		stats, _, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: client.Gauge,
					value: 10,
				},
			}, time.Second*10,
		)

		errors := stats.Errors()
		if len(errors) != 0 {
			tt.Fatalf("expected to find zero errors, have %d", len(errors))
		}
	})

	t.Run("error", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		testApi.sendSeriesError = fmt.Errorf("failed sent")

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: client.Gauge,
					value: 10,
				},
			}, time.Second*10,
		)

		errors := stats.Errors()
		if len(errors) != 1 {
			tt.Fatalf("expected to find 1 error, have %d", len(errors))
		}

		if errors[0].Error() != testApi.sendSeriesError.Error() {
			tt.Fatalf("expected error to be %s, have %s", testApi.sendSeriesError.Error(), errors[0].Error())
		}
	})

}

func TestStats_FlushCallback(t *testing.T) {
	t.Run("flush one stat", func(tt *testing.T) {
		stats, _, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		var callbackStats = make([]*client.DDMetric, 0)
		stats.FlushCallback(func(metricSeries []*client.DDMetric) {
			callbackStats = metricSeries
		})

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: client.Gauge,
					value: 10,
				},
			}, time.Second*10,
		)

		if len(callbackStats) != 1 {
			tt.Fatalf("expected there to be stats %d stat in flush call, there was %d", 1, len(callbackStats))
		}
	})
}

func TestStats_ErrorCallback(t *testing.T) {

	t.Run("no error", func(tt *testing.T) {
		stats, _, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}

		var callbackError error
		stats.ErrorCallback(func(err error, metricSeries []*client.DDMetric) {
			callbackError = err
		})

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: client.Gauge,
					value: 10,
				},
			}, time.Second*10,
		)

		if callbackError != nil {
			tt.Fatalf("expected to have no error sent to the callback, have %s", callbackError.Error())
		}
	})

	t.Run("error", func(tt *testing.T) {
		stats, testApi, err := NewTestStatsWithStart()
		if err != nil {
			tt.Fatalf(err.Error())
		}
		testApi.sendSeriesError = fmt.Errorf("failed sent")

		var callbackError error
		var metricSeries []*client.DDMetric
		stats.ErrorCallback(func(err error, m []*client.DDMetric) {
			callbackError = err
			metricSeries = m
		})

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: client.Gauge,
					value: 10,
				},
			}, time.Second*10,
		)

		if callbackError == nil {
			tt.Fatalf("expected to have call back called with error, have nil")
		}

		if callbackError.Error() != testApi.sendSeriesError.Error() {
			tt.Fatalf("expected error to be %s, have %s", testApi.sendSeriesError.Error(), callbackError.Error())
		}

		if len(metricSeries) != 1 {
			tt.Fatalf("expected %d metrics to be passed to call back, have %d", 1, len(metricSeries))
		}
	})

}

func TestStats_GetDroppedMetricCount(t *testing.T) {

	t.Run("dropped rate", func(tt *testing.T) {
		stats := &Stats{
			metricUpdates: make(chan *metric, 2),
		}

		if stats.GetDroppedMetricCount() != 0 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 0, stats.GetDroppedMetricCount())
		}

		stats.IncrementRate("one", nil)
		stats.IncrementRate("one", nil)
		stats.IncrementRate("one", nil)

		if stats.GetDroppedMetricCount() != 1 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 1, stats.GetDroppedMetricCount())
		}
	})

	t.Run("dropped count", func(tt *testing.T) {
		stats := &Stats{
			metricUpdates: make(chan *metric, 2),
		}

		if stats.GetDroppedMetricCount() != 0 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 0, stats.GetDroppedMetricCount())
		}

		stats.Increment("one", nil)
		stats.Increment("one", nil)
		stats.Increment("one", nil)

		if stats.GetDroppedMetricCount() != 1 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 1, stats.GetDroppedMetricCount())
		}
	})

	t.Run("dropped gauge", func(tt *testing.T) {
		stats := &Stats{
			metricUpdates: make(chan *metric, 2),
		}

		if stats.GetDroppedMetricCount() != 0 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 0, stats.GetDroppedMetricCount())
		}

		stats.Gauge("one", 1, nil)
		stats.Gauge("one", 1, nil)
		stats.Gauge("one", 1, nil)

		if stats.GetDroppedMetricCount() != 1 {
			tt.Fatalf("expected GetDroppedMetricCount to be %d, have %d", 1, stats.GetDroppedMetricCount())
		}
	})
}

func Test_appendErrorsList(t *testing.T) {

	t.Run("under max", func(tt *testing.T) {

		result := appendErrorsList([]error{fmt.Errorf("one")}, fmt.Errorf("two"), 2)

		if len(result) != 2 {
			tt.Fatalf("expected to have %d errors returned, have %d", 2, len(result))
		}
	})

	t.Run("over max", func(tt *testing.T) {

		result := appendErrorsList([]error{fmt.Errorf("one")}, fmt.Errorf("two"), 1)

		if len(result) != 1 {
			tt.Fatalf("expected to have %d errors returned, have %d", 1, len(result))
		}
		if result[0].Error() != "two" {
			tt.Fatalf("expected first error to be %s, have %s", "two", result[0].Error())
		}
	})
}
