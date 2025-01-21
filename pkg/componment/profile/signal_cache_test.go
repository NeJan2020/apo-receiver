package profile

import (
	"testing"

	"github.com/CloudDetail/apo-module/model/v1"
	"github.com/stretchr/testify/assert"
)

func TestQuerySilentSwitches(t *testing.T) {
	cache := newSignalCache()

	// Prepare Data
	trace_one_profiled := &model.Trace{
		Labels: &model.TraceLabels{
			Pid:         1,
			Url:         "/t",
			ServiceName: "T",
			IsProfiled:  true,
		},
	}
	trace_one_not_profiled := &model.Trace{
		Labels: &model.TraceLabels{
			Pid:         1,
			Url:         "/t",
			ServiceName: "T",
			IsProfiled:  false,
		},
	}
	trace_two_profiled := &model.Trace{
		Labels: &model.TraceLabels{
			Pid:         2,
			Url:         "/s",
			ServiceName: "T",
			IsProfiled:  true,
		},
	}
	trace_two_not_profiled := &model.Trace{
		Labels: &model.TraceLabels{
			Pid:         2,
			Url:         "/s",
			ServiceName: "T",
			IsProfiled:  false,
		},
	}

	cache.addSignal("A", "/a", trace_one_profiled, false)
	checkSwitchResult(t, cache, []string{}, []string{})

	cache.addSignal("A", "/a", trace_one_not_profiled, false)
	checkSwitchResult(t, cache, []string{}, []string{})

	cache.addSignal("A", "/a", trace_two_not_profiled, false)
	cache.addSignal("B", "/b", trace_one_not_profiled, false)
	checkSwitchResult(t, cache, []string{"1-/t", "2-/s"}, []string{})

	cache.addSignal("C", "/c", trace_one_not_profiled, false)
	checkSwitchResult(t, cache, []string{"1-/t"}, []string{})

	cache.addSignal("C", "/c", trace_one_profiled, false)
	checkSwitchResult(t, cache, []string{}, []string{})

	cache.addSignal("B", "/b", trace_one_profiled, false)
	checkSwitchResult(t, cache, []string{}, []string{"1-/t"})

	cache.addSignal("A", "/a", trace_two_profiled, false)
	checkSwitchResult(t, cache, []string{}, []string{"2-/s"})
}

func checkSwitchResult(t *testing.T, cache *SignalCache, close []string, recover []string) {
	left, right := cache.querySilentSwitches()
	assert.Equal(
		t,
		close,
		left,
		"Unexpected close silent",
	)
	assert.Equal(
		t,
		recover,
		right,
		"Unexpected recover silent",
	)
}
