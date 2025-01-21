package pm

import (
	"io"

	pb "github.com/CloudDetail/apo-receiver/internal/prometheus"
	"github.com/CloudDetail/apo-receiver/pkg/metrics/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type PromGauge struct {
	PromMetric
	gauge prometheus.Gauge
}

func NewPromGauge(def *model.MetricDef, labelValues []string) *PromGauge {
	return &PromGauge{
		gauge: prometheus.NewGauge(prometheus.GaugeOpts{
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

func (c *PromGauge) Update(v float64) {
	c.gauge.Add(v)
}

func (prom *PromGauge) MarshalTo(name string, labelKey string, w io.Writer) error {
	metric := &dto.Metric{}
	if err := prom.gauge.Write(metric); err != nil {
		return err
	}
	if _, err := writeSample(w, name, "", labelKey, metric.Gauge.GetValue()); err != nil {
		return err
	}
	return nil
}

func (prom *PromGauge) ExportTimeSeries(ts int64, series *[]*pb.TimeSeries) error {
	metric := &dto.Metric{}
	if err := prom.gauge.Write(metric); err != nil {
		return err
	}
	*series = append(*series, newPromTimeSeries(ts, prom.PromMetric, "", metric.Gauge.GetValue()))
	return nil
}
