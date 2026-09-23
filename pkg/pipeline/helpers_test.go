package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/normalize"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/sampling"
)

func cfgSampling(ratio float64, cap int) sampling.Config {
	return sampling.Config{Ratio: ratio, CacheCap: cap}
}

func shutdownCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = cancel
	return ctx
}

// httpToW3C converts an HTTP fixture batch into the equivalent W3C source
// payload (same trace/span ids, ts+dur timing).
func httpToW3C(t *testing.T, in []normalize.HTTPSpanV1) []normalize.W3CSpanV1 {
	t.Helper()
	out := make([]normalize.W3CSpanV1, 0, len(in))
	for _, h := range in {
		tid, err := model.ParseTraceID(h.TraceID)
		if err != nil {
			t.Fatal(err)
		}
		sid, err := model.ParseSpanID(h.SpanID)
		if err != nil {
			t.Fatal(err)
		}
		kind := 0
		switch h.Kind {
		case "server":
			kind = int(model.KindServer)
		case "client":
			kind = int(model.KindClient)
		}
		ok := true
		w := normalize.W3CSpanV1{
			Version:     0,
			TraceParent: "00-" + tid.String() + "-" + sid.String() + "-01",
			Name:        h.Name, Kind: kind,
			StartNano:    h.StartUnixNano,
			DurationNano: h.EndUnixNano - h.StartUnixNano,
			OK:           &ok,
		}
		if h.ParentSpanID != "" {
			pid, err := model.ParseSpanID(h.ParentSpanID)
			if err != nil {
				t.Fatal(err)
			}
			w.Parent = pid.String()
		}
		out = append(out, w)
	}
	return out
}

// httpToZipkin converts the HTTP fixture batch into Zipkin-shape payload.
func httpToZipkin(t *testing.T, in []normalize.HTTPSpanV1) []normalize.ZipkinSpanV1 {
	t.Helper()
	out := make([]normalize.ZipkinSpanV1, 0, len(in))
	for _, h := range in {
		z := normalize.ZipkinSpanV1{
			TraceID:        h.TraceID,
			ID:             h.SpanID,
			ParentID:       h.ParentSpanID,
			Name:           h.Name,
			Kind:           upperKind(h.Kind),
			TimestampMicro: h.StartUnixNano / 1000,
			DurationMicro:  (h.EndUnixNano - h.StartUnixNano) / 1000,
		}
		out = append(out, z)
	}
	return out
}

func upperKind(k string) string {
	switch k {
	case "server":
		return "SERVER"
	case "client":
		return "CLIENT"
	default:
		return ""
	}
}
