package otlp

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	collectorTrace "github.com/opsramp/husky/proto/otlp/collector/trace/v1"
	trace "github.com/opsramp/husky/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
	defaultSampleRate  = int32(1)
)

// TranslateTraceRequestResult represents an OTLP trace request translated into Opsramp-friendly structure
// RequestSize is total byte size of the entire OTLP request
// Batches represent events grouped by their target dataset
type TranslateTraceRequestResult struct {
	RequestSize int
	Batches     []Batch
}

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateTraceRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	request := &collectorTrace.ExportTraceServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request); err != nil {
		return nil, fmt.Errorf("%s: %s", ErrFailedParseBody, err)
	}
	return TranslateTraceRequest(request, ri)
}

// TranslateTraceRequest translates an OTLP/gRPC request into OpsRamp-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateTraceRequest(request *collectorTrace.ExportTraceServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	var batches []Batch

	for _, resourceSpan := range request.ResourceSpans {
		var events []Event
		resourceAttrs := getResourceAttributes(resourceSpan.Resource)
		dataset := getDataset(ri, resourceAttrs)
		traceAttributes := make(map[string]map[string]interface{})
		traceAttributes["resourceAttributes"] = make(map[string]interface{})

		// trying to classify the spans based on resource attributes
		_classificationAttributes := map[string]string{}

		if resourceSpan.Resource != nil {
			addAttributesToMap(traceAttributes["resourceAttributes"], resourceSpan.Resource.Attributes)
			_classificationAttributes = DetermineClassification(resourceSpan.GetResource().GetAttributes())
		}

		for _, librarySpan := range resourceSpan.ScopeSpans {
			scopeAttrs := getScopeAttributes(librarySpan.Scope)
			library := librarySpan.Scope
			if library != nil {
				if len(library.Name) > 0 {
					traceAttributes["resourceAttributes"]["library.name"] = library.Name
				}
				if len(library.Version) > 0 {
					traceAttributes["resourceAttributes"]["library.version"] = library.Version
				}
			}

			// update classification attrs with scope attributes
			_scopeClassificationAttrs := _classificationAttributes
			if librarySpan.GetScope() != nil {
				_scopeClassificationAttrs = NormalizeClassification(_classificationAttributes, librarySpan.GetScope().GetAttributes())
			}

			for _, span := range librarySpan.GetSpans() {

				traceAttributes["spanAttributes"] = make(map[string]interface{})
				traceAttributes["eventAttributes"] = make(map[string]interface{})

				traceID := BytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)

				spanKind := getSpanKind(span.Kind)
				statusCode, isError := getSpanStatusCode(span.Status)

				eventAttrs := map[string]interface{}{
					"traceTraceID":     traceID,
					"traceSpanID":      spanID,
					"type":             spanKind,
					"spanKind":         spanKind,
					"spanName":         span.Name,
					"durationMs":       float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"startTime":        int64(span.StartTimeUnixNano),
					"endTime":          int64(span.EndTimeUnixNano),
					"statusCode":       statusCode,
					"spanNumLinks":     len(span.Links),
					"spanNumEvents":    len(span.Events),
					"meta.signal_type": "trace",
				}
				if span.ParentSpanId != nil {
					eventAttrs["traceParentID"] = hex.EncodeToString(span.ParentSpanId)
				}

				eventAttrs["error"] = isError
				if isError {
					traceAttributes["resourceAttributes"]["error"] = isError
				}

				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["statusMessage"] = span.Status.Message
				}

				// copy resource & scope attributes then span attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}
				for k, v := range scopeAttrs {
					eventAttrs[k] = v
				}

				// update scope classification attrs with span attributes
				if span.Attributes != nil {
					addAttributesToMap(traceAttributes["spanAttributes"], span.Attributes)
					_spanClassificationAttrs := NormalizeClassification(_scopeClassificationAttrs, span.GetAttributes())

					for k, v := range _spanClassificationAttrs {
						traceAttributes["spanAttributes"][k] = v
					}
				}

				// get sample rate after resource and scope attributes have been added
				sampleRate := getSampleRate(eventAttrs)

				//Copy resource attributes
				eventAttrs["resourceAttributes"] = traceAttributes["resourceAttributes"]

				//Copy span attributes
				eventAttrs["spanAttributes"] = traceAttributes["spanAttributes"]

				//Check for event attributes and add them
				for _, sevent := range span.Events {
					if sevent.Attributes != nil {
						addAttributesToMap(traceAttributes["eventAttributes"], sevent.Attributes)
					}
				}
				eventAttrs["eventAttributes"] = traceAttributes["eventAttributes"]

				eventAttrs["time"] = int64(span.StartTimeUnixNano)
				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()
				events = append(events, Event{
					Attributes: eventAttrs,
					Timestamp:  timestamp,
					SampleRate: sampleRate,
				})
			}
		}
		batches = append(batches, Batch{
			Dataset:   dataset,
			SizeBytes: proto.Size(resourceSpan),
			Events:    events,
		})
	}
	return &TranslateOTLPRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

func getSpanKind(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_CLIENT:
		return "client"
	case trace.Span_SPAN_KIND_SERVER:
		return "server"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	case trace.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case trace.Span_SPAN_KIND_UNSPECIFIED:
		fallthrough
	default:
		return "unspecified"
	}
}

// BytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// There are many ways to achieve this, but careful benchmarking and testing
// showed the below as the most performant, avoiding memory allocations
// and the use of flexible but expensive library functions. As this is hot code,
// it seemed worthwhile to do it this way.
func BytesToTraceID(traceID []byte) string {
	var encoded []byte
	switch len(traceID) {
	case traceIDLongLength: // 16 bytes, trim leading 8 bytes if all 0's
		if shouldTrimTraceId(traceID) {
			encoded = make([]byte, 16)
			traceID = traceID[traceIDShortLength:]
		} else {
			encoded = make([]byte, 32)
		}
	case traceIDShortLength: // 8 bytes
		encoded = make([]byte, 16)
	default:
		encoded = make([]byte, len(traceID)*2)
	}
	hex.Encode(encoded, traceID)
	return string(encoded)
}

func shouldTrimTraceId(traceID []byte) bool {
	for i := 0; i < 8; i++ {
		if traceID[i] != 0 {
			return false
		}
	}
	return true
}

// getSpanStatusCode returns the integer value of the span's status code and
// a bool for whether to consider the status an error.
//
// The type conversion from proto enum value to an integer is done here because
// the events we produce from OTLP spans have no knowledge of or interest in
// the OTLP types generated from enums in the proto definitions.
func getSpanStatusCode(status *trace.Status) (int, bool) {
	if status == nil {
		return int(trace.Status_STATUS_CODE_UNSET), false
	}
	return int(status.Code), status.Code == trace.Status_STATUS_CODE_ERROR
}

func getSampleRate(attrs map[string]interface{}) int32 {
	sampleRateKey := getSampleRateKey(attrs)
	if sampleRateKey == "" {
		return defaultSampleRate
	}

	sampleRate := defaultSampleRate
	sampleRateVal := attrs[sampleRateKey]
	switch v := sampleRateVal.(type) {
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			if i < math.MaxInt32 {
				sampleRate = int32(i)
			} else {
				sampleRate = math.MaxInt32
			}
		}
	case int32:
		sampleRate = v
	case int:
		if v < math.MaxInt32 {
			sampleRate = int32(v)
		} else {
			sampleRate = math.MaxInt32
		}
	case int64:
		if v < math.MaxInt32 {
			sampleRate = int32(v)
		} else {
			sampleRate = math.MaxInt32
		}
	}
	// To make sampleRate consistent between Otel and Honeycomb, we coerce all 0 values to 1 here
	// A value of 1 means the span was not sampled
	// For full explanation, see https://app.asana.com/0/365940753298424/1201973146987622/f
	if sampleRate == 0 {
		sampleRate = defaultSampleRate
	}
	delete(attrs, sampleRateKey) // remove attr
	return sampleRate
}

func getSampleRateKey(attrs map[string]interface{}) string {
	if _, ok := attrs["sampleRate"]; ok {
		return "sampleRate"
	}
	if _, ok := attrs["SampleRate"]; ok {
		return "SampleRate"
	}
	return ""
}
