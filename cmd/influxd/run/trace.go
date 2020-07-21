package run

import (
	"fmt"
	"github.com/influxdata/flux"
	"github.com/opentracing/opentracing-go"
	config "github.com/uber/jaeger-client-go/config"
	"io"
	"time"
)

func init() {

		flux.EnableExperimentalTracing()
		tracer, _ := initJaeger("flux")
		opentracing.SetGlobalTracer(tracer)


}
func initJaeger(service string) (opentracing.Tracer, io.Closer) {
	cfg := &config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            true,
			LocalAgentHostPort:  "10.227.5.103:5775",
			BufferFlushInterval: time.Second,
		},
	}
	tracer, closer, err := cfg.New(service)
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return tracer, closer
}
