package pm

import (
	"io"
	"math"
	"strconv"

	pb "github.com/CloudDetail/apo-receiver/internal/prometheus"
	"github.com/CloudDetail/apo-receiver/pkg/metrics/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type PromHistogram struct {
	PromMetric
	histogram prometheus.Histogram
}

func NewPromHistogram(def *model.MetricDef, labelValues []string, buckets []float64) *PromHistogram {
	return &PromHistogram{
		histogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    def.Name,
			Help:    def.Help,
			Buckets: buckets,
		}),
		PromMetric: PromMetric{
			Name:   def.Name,
			Keys:   def.Keys,
			Values: labelValues,
		},
	}
}

func (prom *PromHistogram) Update(value float64) {
	prom.histogram.Observe(value)
}

func (prom *PromHistogram) MarshalTo(name string, labelKey string, w io.Writer) error {
	metric := &dto.Metric{}
	if err := prom.histogram.Write(metric); err != nil {
		return err
	}
	histogram := metric.Histogram
	infSeen := false
	for _, bucket := range histogram.Bucket {
		inf, bucketValue := getPromBucketValue(bucket.GetUpperBound())
		if inf {
			infSeen = true
		}
		if _, err := writeAdditionalSample(w, name, "_bucket", labelKey, "le", bucketValue, float64(bucket.GetCumulativeCount())); err != nil {
			return err
		}
	}
	if !infSeen {
		if _, err := writeAdditionalSample(w, name, "_bucket", labelKey, "le", "+Inf", float64(histogram.GetSampleCount())); err != nil {
			return err
		}
	}

	if _, err := writeSample(w, name, "_sum", labelKey, histogram.GetSampleSum()); err != nil {
		return err
	}
	if _, err := writeSample(w, name, "_count", labelKey, float64(histogram.GetSampleCount())); err != nil {
		return err
	}
	return nil
}

func (prom *PromHistogram) ExportTimeSeries(ts int64, series *[]*pb.TimeSeries) error {
	metric := &dto.Metric{}
	if err := prom.histogram.Write(metric); err != nil {
		return err
	}

	histogram := metric.Histogram
	infSeen := false
	for _, bucket := range histogram.Bucket {
		inf, bucketValue := getPromBucketValue(bucket.GetUpperBound())
		if inf {
			infSeen = true
		}
		*series = append(*series, newPromTimeSeriesWithAdditional(ts, prom.PromMetric, "_bucket", "le", bucketValue, float64(bucket.GetCumulativeCount())))
	}
	if !infSeen {
		*series = append(*series, newPromTimeSeriesWithAdditional(ts, prom.PromMetric, "_bucket", "le", "+Inf", float64(histogram.GetSampleCount())))
	}
	*series = append(*series, newPromTimeSeries(ts, prom.PromMetric, "_sum", histogram.GetSampleSum()))
	*series = append(*series, newPromTimeSeries(ts, prom.PromMetric, "_count", float64(histogram.GetSampleCount())))
	return nil
}

func getPromBucketValue(value float64) (bool, string) {
	if math.IsInf(value, +1) {
		return true, "+Inf"
	}
	return false, strconv.FormatInt(int64(value), 10)
}
