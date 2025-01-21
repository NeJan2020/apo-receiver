package threshold

type ThresholdType string

const (
	P90         ThresholdType = "LatencyP90"
	P95         ThresholdType = "LatencyP95"
	P99         ThresholdType = "LatencyP99"
	UnknownType               = "unknown"
)

type ThresholdRange string

const (
	Last1h       ThresholdRange = "last1h"
	Yesterday                   = "yesterday"
	Constant                    = "constant"
	Default                     = "default"
	UnknownRange                = "unknown"
)

func (thresholdRange ThresholdRange) String() string {
	switch thresholdRange {
	case Last1h:
		return "1h"
	case Yesterday:
		return "24h"
	case Constant:
		return "constant"
	case Default:
		return "default"
	default:
		return "unknown"
	}
}

func ToThresholdType(percentile float64) ThresholdType {
	switch percentile {
	case 0.9:
		return P90
	case 0.95:
		return P95
	case 0.99:
		return P99
	default:
		return UnknownType
	}
}

func ToThresholdRange(duration string) ThresholdRange {
	switch duration {
	case "1h":
		return Last1h
	case "24h":
		return Yesterday
	case "1d":
		return Yesterday
	default:
		return UnknownRange
	}
}
