package pm

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
)

const (
	initialNumBufSize = 24
)

var (
	numBufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, initialNumBufSize)
			return &b
		},
	}
)

func writeSample(w io.Writer, name string, suffix string, labelKey string, value float64) (int, error) {
	if labelKey == "" {
		return fmt.Fprintf(w, "%s%s %s\n", name, suffix, formatFloat(value))
	} else {
		return fmt.Fprintf(w, "%s%s{%s} %s\n", name, suffix, labelKey, formatFloat(value))
	}
}

func writeAdditionalSample(w io.Writer, name string, suffix string, labelKey string, additionalLabelName string, additionalLabelValue string, value float64) (int, error) {
	if labelKey == "" {
		return fmt.Fprintf(w, "%s%s{%s=%q} %s\n", name, suffix, additionalLabelName, additionalLabelValue, formatFloat(value))
	} else {
		return fmt.Fprintf(w, "%s%s{%s,%s=%q} %s\n", name, suffix, labelKey, additionalLabelName, additionalLabelValue, formatFloat(value))
	}
}

func formatFloat(f float64) string {
	switch {
	case f == 1:
		return "1"
	case f == 0:
		return "0"
	case f == -1:
		return "-1"
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, +1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	default:
		bp := numBufPool.Get().(*[]byte)
		*bp = strconv.AppendFloat((*bp)[:0], f, 'g', -1, 64)
		result := string(*bp)
		numBufPool.Put(bp)
		return result
	}
}
