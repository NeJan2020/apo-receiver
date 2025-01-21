package pm

import (
	"io"

	pb "github.com/CloudDetail/apo-receiver/internal/prometheus"
	"github.com/CloudDetail/apo-receiver/pkg/metrics/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type PromCounter struct {
	PromMetric
	counter prometheus.Counter
}

func NewPromCounter(def *model.MetricDef, labelValues []string) *PromCounter {
	return &PromCounter{
		counter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: def.Name,
			Help: def.Help,
		}),
		PromMetric: PromMetric{
			Name:   def.Name,
			Keys:   def.Keys,
			Values: labelValues,
		},
	}
}

func (c *PromCounter) Update(v float64) {
	c.counter.Add(v)
}

func (prom *PromCounter) MarshalTo(name string, labelKey string, w io.Writer) error {
	metric := &dto.Metric{}
	if err := prom.counter.Write(metric); err != nil {
		return err
	}
	if _, err := writeSample(w, name, "", labelKey, metric.Counter.GetValue()); err != nil {
		return err
	}
	return nil
}

func (prom *PromCounter) ExportTimeSeries(ts int64, series *[]*pb.TimeSeries) error {
	metric := &dto.Metric{}
	if err := prom.counter.Write(metric); err != nil {
		return err
	}
	*series = append(*series, newPromTimeSeries(ts, prom.PromMetric, "", metric.Counter.GetValue()))
	return nil
}
