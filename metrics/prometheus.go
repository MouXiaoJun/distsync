// Package metrics provides a Prometheus implementation of distsync.Metrics.
//
//	client := distsync.New(rdb, distsync.WithMetrics(metrics.New(nil)))
//
// Exported collectors:
//
//	distsync_acquires_total{primitive, resource, result}
//	distsync_acquire_wait_seconds{primitive, resource}   (histogram)
//	distsync_releases_total{primitive, resource}
//	distsync_renews_total{primitive, resource, result}
//	distsync_renewal_stopped_total{primitive, resource, reason}
package metrics

import (
	"time"

	"github.com/MouXiaoJun/distsync"
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus implements distsync.Metrics with Prometheus collectors. Create it
// once per process and share it across Clients.
type Prometheus struct {
	resources    map[string]struct{}
	acquires     *prometheus.CounterVec
	acquireWait  *prometheus.HistogramVec
	releases     *prometheus.CounterVec
	renews       *prometheus.CounterVec
	renewStopped *prometheus.CounterVec
}

// New builds a Prometheus metrics sink and registers its collectors. Pass a
// custom *prometheus.Registry for isolated testing; nil registers on the
// process-wide default registry. All resource names are aggregated as "other".
func New(reg prometheus.Registerer) *Prometheus {
	return NewWithResources(reg)
}

// NewWithResources keeps resource labels only for this fixed allowlist; all
// other names become "other". The list is copied at construction and never
// grows at runtime. With N distinct allowed names, each collector has at most
// N+1 resource label values. Prefer a small set of stable, non-sensitive names.
// Primitive and reason labels come from distsync's finite lifecycle vocabulary;
// callers invoking the sink directly must keep those labels bounded too.
func NewWithResources(reg prometheus.Registerer, resources ...string) *Prometheus {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	p := &Prometheus{
		resources: make(map[string]struct{}, len(resources)),
		acquires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "distsync",
			Name:      "acquires_total",
			Help:      "Number of lease acquisition attempts by result.",
		}, []string{"primitive", "resource", "result"}),
		acquireWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "distsync",
			Name:      "acquire_wait_seconds",
			Help:      "Time spent waiting to acquire a lease.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"primitive", "resource"}),
		releases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "distsync",
			Name:      "releases_total",
			Help:      "Number of lease releases.",
		}, []string{"primitive", "resource"}),
		renews: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "distsync",
			Name:      "renews_total",
			Help:      "Number of lease renewals by result.",
		}, []string{"primitive", "resource", "result"}),
		renewStopped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "distsync",
			Name:      "renewal_stopped_total",
			Help:      "Number of times background renewal ended, by reason.",
		}, []string{"primitive", "resource", "reason"}),
	}
	for _, resource := range resources {
		p.resources[resource] = struct{}{}
	}
	reg.MustRegister(p.acquires, p.acquireWait, p.releases, p.renews, p.renewStopped)
	return p
}

func (p *Prometheus) resourceLabel(resource string) string {
	if _, ok := p.resources[resource]; ok {
		return resource
	}
	return "other"
}

// Acquire implements distsync.Metrics.
func (p *Prometheus) Acquire(primitive, resource string, ok bool, wait time.Duration) {
	resource = p.resourceLabel(resource)
	result := "success"
	if !ok {
		result = "failure"
	}
	p.acquires.WithLabelValues(primitive, resource, result).Inc()
	p.acquireWait.WithLabelValues(primitive, resource).Observe(wait.Seconds())
}

// Release implements distsync.Metrics.
func (p *Prometheus) Release(primitive, resource string) {
	resource = p.resourceLabel(resource)
	p.releases.WithLabelValues(primitive, resource).Inc()
}

// Renew implements distsync.Metrics.
func (p *Prometheus) Renew(primitive, resource string, ok bool) {
	resource = p.resourceLabel(resource)
	result := "success"
	if !ok {
		result = "failure"
	}
	p.renews.WithLabelValues(primitive, resource, result).Inc()
}

// RenewalStopped implements distsync.Metrics.
func (p *Prometheus) RenewalStopped(primitive, resource, reason string) {
	resource = p.resourceLabel(resource)
	p.renewStopped.WithLabelValues(primitive, resource, reason).Inc()
}

// compile-time check that Prometheus implements distsync.Metrics.
var _ distsync.Metrics = (*Prometheus)(nil)
