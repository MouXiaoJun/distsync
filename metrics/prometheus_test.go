package metrics

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestResourceLabelsBounded(t *testing.T) {
	for _, allow := range [][]string{nil, {"scheduler", "settlement"}} {
		t.Run(strconv.Itoa(len(allow)), func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			var sink *Prometheus
			if allow == nil {
				sink = New(reg)
			} else {
				sink = NewWithResources(reg, allow...)
				allow[0] = "mutated" // constructor must not retain the caller's slice
			}
			record := func(resource string) {
				sink.Acquire("mutex", resource, true, time.Millisecond)
				sink.Release("mutex", resource)
				sink.Renew("mutex", resource, true)
				sink.RenewalStopped("mutex", resource, "released")
			}
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						record("order:" + strconv.Itoa(i))
					}
				}()
			}
			wg.Wait()
			record("scheduler")
			record("settlement")
			families, err := reg.Gather()
			if err != nil {
				t.Fatal(err)
			}
			if len(families) != 5 {
				t.Fatalf("collectors = %d, want 5", len(families))
			}
			for _, family := range families {
				if len(family.Metric) != len(allow)+1 {
					t.Fatalf("%s series = %d, want %d", family.GetName(), len(family.Metric), len(allow)+1)
				}
				var total float64
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "resource" {
							v := label.GetValue()
							allowed := len(allow) > 0 && (v == "scheduler" || v == "settlement")
							if v != "other" && !allowed {
								t.Fatalf("unexpected resource %q", v)
							}
						}
					}
					if metric.Histogram != nil {
						total += float64(metric.Histogram.GetSampleCount())
					} else {
						total += metric.Counter.GetValue()
					}
				}
				if total != 8002 {
					t.Fatalf("%s lost events: %g", family.GetName(), total)
				}
			}
		})
	}
}
