package ddstats

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type TestAPIClient struct {
	series []*DDMetricSeries
	checks []*DDServiceCheck
	events []*DDEvent
	lock   *sync.Mutex

	sendEventError  error
	sendSeriesError error
}

func NewTestAPIClient() *TestAPIClient {
	return &TestAPIClient{
		series: []*DDMetricSeries{},
		checks: []*DDServiceCheck{},
		events: []*DDEvent{},
		lock:   &sync.Mutex{},
	}
}

func (t *TestAPIClient) SendSeries(series *DDMetricSeries) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.series = append(t.series, series)
	return t.sendSeriesError
}

func (t *TestAPIClient) SendServiceCheck(check *DDServiceCheck) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.checks = append(t.checks, check)
	return nil
}

func (t *TestAPIClient) SendEvent(event *DDEvent) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.events = append(t.events, event)
	return t.sendEventError
}

func (t *TestAPIClient) SetHTTPClient(client HTTPClient) {}

func (t *TestAPIClient) FindMetric(callIndex int, ddMetric *DDMetric) error {

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

func (t *TestAPIClient) TestValidateCalls(seriesCalls []*DDMetricSeries, checkCalls, eventCalls int) error {

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

func NewTestStats() (*Stats, *TestAPIClient) {

	testClient := NewTestAPIClient()
	s := NewStats(testNamespace, testHost, "", testTags)
	s.Close()
	s.flushInterval = time.Minute * 1
	s.workerCount = 2
	s.workerBuffer = 10
	s.metricBuffer = 10 * 2
	s.client = testClient
	s.ready = make(chan bool, 1)

	return s, testClient
}

func NewTestStatsWithStart() (*Stats, *TestAPIClient) {
	stats, testApi := NewTestStats()
	go stats.start()
	stats.blockReady()
	return stats, testApi
}

func TestStats_SendSeries(t *testing.T) {

	t.Run("one metric", func(tt *testing.T) {
		stats, testApi := NewTestStats()

		metrics := []*DDMetric{
			{
				Host:     "testHost2",
				Interval: 10,
				Metric:   "metric1",
				Points:   [][2]interface{}{{1, 2}},
				Tags:     []string{"tag:2"},
				Type:     metricCount,
			},
		}

		if err := stats.SendSeries(metrics); err != nil {
			tt.Fatalf("send returned err, %s", err.Error())
		}

		seriesCalls := []*DDMetricSeries{
			{
				Series: []*DDMetric{
					{
						Host:     "testHost2",
						Metric:   "testNamespace.metric1",
						Tags:     []string{"tag:2", "tag:1"},
						Interval: 10,
						Type:     metricCount,
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

func TestStats_CountGauge(t *testing.T) {

	baseMetric := DDMetric{
		Host:     testHost,
		Metric:   "testNamespace.test",
		Tags:     []string{"tag:1"},
		Interval: 1,
		Type:     metricCount,
	}

	t.Run("one increment", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Increment("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(1)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two increment", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one decrement", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-1)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("two decrement", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Decrement("test", nil)
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(-2)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("three increment one decrement", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		stats.Increment("test", nil)
		stats.Decrement("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(2)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("count value 10", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Count("test", 10, nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one gauge", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Gauge("test", 10, nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		m1.Type = metricGauge
		m1.Interval = 0
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})

	t.Run("one gauge and one increment", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		stats.Gauge("test2", 10, nil)
		stats.Increment("test", nil)
		time.Sleep(time.Millisecond * 100)
		stats.Close()

		m2 := baseMetric
		m2.Points = [][2]interface{}{{1, float64(1)}}
		m2.Metric = "testNamespace.test"
		m1 := baseMetric
		m1.Points = [][2]interface{}{{1, float64(10)}}
		m1.Metric = "testNamespace.test2"
		m1.Type = metricGauge
		m1.Interval = 0
		seriesCalls := []*DDMetricSeries{{Series: []*DDMetric{&m1, &m2}}}

		if err := testApi.TestValidateCalls(seriesCalls, 0, 0); err != nil {
			tt.Fatalf(err.Error())
		}
	})
}

func TestStats_Event(t *testing.T) {

	testEvent := DDEvent{
		AggregationKey: "testEvent",
		AlertType:      AlertError,
		DateHappened:   time.Now().Unix(),
		DeviceName:     "device1",
		Host:           "host1",
		Priority:       PriorityNormal,
		SourceTypeName: "",
		Tags:           nil,
		Title:          "",
	}

	t.Run("no error", func(tt *testing.T) {
		stats, testApi := NewTestStats()
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

	t.Run("error", func(tt *testing.T) {
		stats, testApi := NewTestStats()
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
		stats, testApi := NewTestStats()
		if err := stats.ServiceCheck("check1", "failed", Warning, nil); err != nil {
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

		if testApi.checks[0].Status != Warning {
			tt.Fatalf("expected check message to be %d, have %d", Warning, testApi.checks[0].Status)
		}
	})
}

func TestStats_Errors(t *testing.T) {

	t.Run("no error", func(tt *testing.T) {
		stats, _ := NewTestStatsWithStart()

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: metricGauge,
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
		stats, testApi := NewTestStatsWithStart()
		testApi.sendSeriesError = fmt.Errorf("failed sent")

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: metricGauge,
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

func TestStats_ErrorCallback(t *testing.T) {

	t.Run("no error", func(tt *testing.T) {
		stats, _ := NewTestStatsWithStart()

		var callbackError error
		stats.ErrorCallback(func(err error, metricSeries []*DDMetric) {
			callbackError = err
		})

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: metricGauge,
					value: 10,
				},
			}, time.Second*10,
		)

		if callbackError != nil {
			tt.Fatalf("expected to have no error sent to the callback, have %s", callbackError.Error())
		}
	})

	t.Run("error", func(tt *testing.T) {
		stats, testApi := NewTestStatsWithStart()
		testApi.sendSeriesError = fmt.Errorf("failed sent")

		var callbackError error
		var metricSeries []*DDMetric
		stats.ErrorCallback(func(err error, m []*DDMetric) {
			callbackError = err
			metricSeries = m
		})

		stats.flushWG.Add(1)
		stats.send(
			map[string]*metric{
				"test": {
					name:  "test",
					class: metricGauge,
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
