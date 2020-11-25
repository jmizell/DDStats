package ddstats

import (
	"testing"
	"time"

	"github.com/jmizell/ddstats/client"
)

func TestMetricUpdate(t *testing.T) {

	t.Run("count add", func(tt *testing.T) {
		m := &metric{class: client.Count}
		m.update(1)
		if m.value != 1.0 {
			tt.Fatalf("expected %f, have %f", 1.0, m.value)
		}
	})

	t.Run("count subtract", func(tt *testing.T) {
		m := &metric{class: client.Count}
		m.update(-1.0)
		if m.value != -1.0 {
			tt.Fatalf("expected %f, have %f", -1.0, m.value)
		}
	})

	t.Run("count arbitrary add", func(tt *testing.T) {
		m := &metric{class: client.Count}
		m.update(10)
		if m.value != 10.0 {
			tt.Fatalf("expected %f, have %f", 10.0, m.value)
		}
	})

	t.Run("count add subtract", func(tt *testing.T) {
		m := &metric{class: client.Count}
		m.update(10)
		m.update(-5)
		if m.value != 5.0 {
			tt.Fatalf("expected %f, have %f", 5.0, m.value)
		}
	})

	t.Run("gauge update", func(tt *testing.T) {
		m := &metric{class: client.Gauge}
		m.update(10)
		if m.value != 10.0 {
			tt.Fatalf("expected %f, have %f", 10.0, m.value)
		}
	})

	t.Run("gauge update multiple", func(tt *testing.T) {
		m := &metric{class: client.Gauge}
		m.update(10)
		m.update(-5)
		if m.value != -5.0 {
			tt.Fatalf("expected %f, have %f", -5.0, m.value)
		}
	})
}

func TestMetricGetMetric(t *testing.T) {

	t.Run("rate 1 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Rate, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*1)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 10.0 {
			tt.Fatalf("expected data point to be %f, have %f", 10.0, ddm.Points[0][1])
		}
		if ddm.Interval != int64((time.Second * 1).Seconds()) {
			tt.Fatalf("expected interval to be %d, have %d", int64((time.Second * 1).Seconds()), ddm.Interval)
		}
	})

	t.Run("rate 5 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Rate, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*5)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 2.0 {
			tt.Fatalf("expected data point to be %f, have %f", 2.0, ddm.Points[0][1])
		}
		if ddm.Interval != int64((time.Second * 5).Seconds()) {
			tt.Fatalf("expected interval to be %d, have %d", int64((time.Second * 5).Seconds()), ddm.Interval)
		}
	})

	t.Run("count 1 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Count, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*1)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 10.0 {
			tt.Fatalf("expected data point to be %f, have %f", 10.0, ddm.Points[0][1])
		}
		if ddm.Interval != int64((time.Second * 1).Seconds()) {
			tt.Fatalf("expected interval to be %d, have %d", int64((time.Second * 1).Seconds()), ddm.Interval)
		}
	})

	t.Run("count 5 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Count, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*5)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 10.0 {
			tt.Fatalf("expected data point to be %f, have %f", 10.0, ddm.Points[0][1])
		}
		if ddm.Interval != int64((time.Second * 5).Seconds()) {
			tt.Fatalf("expected interval to be %d, have %d", int64((time.Second * 5).Seconds()), ddm.Interval)
		}
	})

	t.Run("gauge 1 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Gauge, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*1)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 10.0 {
			tt.Fatalf("expected data point to be %f, have %f", 10.0, ddm.Points[0][1])
		}
		if ddm.Interval != 0 {
			tt.Fatalf("expected interval to be %d, have %d", 0, ddm.Interval)
		}
	})

	t.Run("gauge 5 sec interval", func(tt *testing.T) {
		m := &metric{class: client.Gauge, value: 10}
		ddm := m.getMetric("", "", nil, time.Second*5)
		if len(ddm.Points) != 1 {
			tt.Fatalf("expected to have %d points, have %d", 1, len(ddm.Points))
		}
		if ddm.Points[0][1] != 10.0 {
			tt.Fatalf("expected data point to be %f, have %f", 10.0, ddm.Points[0][1])
		}
		if ddm.Interval != 0 {
			tt.Fatalf("expected interval to be %d, have %d", 0, ddm.Interval)
		}
	})
}
