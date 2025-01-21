package metrics

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/CloudDetail/apo-receiver/internal/prometheus"
	"github.com/CloudDetail/apo-receiver/pkg/metrics/pm"
	"github.com/CloudDetail/apo-receiver/pkg/metrics/vm"
)

type Sender interface {
	SendMetrics(ctx context.Context) error
}

func InitMetricSend(url string, interval int, promType string) error {
	var (
		sender Sender
		err    error
	)

	if promType == "vm" {
		collectMetrics := func(w io.Writer) {
			GetMetrics(w)
		}
		sender, err = vm.NewVmPusher(url, collectMetrics)
	} else {
		buildMetricRequest := func() *pb.WriteRequest {
			return BuildPromWriteRequest()
		}
		sender, err = pm.NewPromRemoteWriter(url, buildMetricRequest)
	}

	if err != nil {
		return err
	}
	return InitMetricSendWithOptions(context.Background(), time.Duration(interval)*time.Second, sender)
}

func InitMetricSendWithOptions(ctx context.Context, interval time.Duration, sender Sender) error {
	// validate interval
	if interval <= 0 {
		return fmt.Errorf("interval must be positive; got %s", interval)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		stopCh := ctx.Done()
		for {
			select {
			case <-ticker.C:
				ctxLocal, cancel := context.WithTimeout(ctx, interval+time.Second)
				err := sender.SendMetrics(ctxLocal)
				cancel()
				if err != nil {
					log.Printf("[x Send Metrics] %s", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	return nil
}
