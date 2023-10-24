package otlp

import (
	v11 "github.com/opsramp/husky/proto/otlp/common/v1"
	"strings"
)

const (
	TransactionType     = "transaction.type"
	_webTransaction     = "web"
	_nonWebTransaction  = "non-web"
	TransactionCategory = "transaction.category"
)

var (
	_categoryAttributes = []string{
		"http.request.method",
		"db.system",
		"messaging.system",
		"rpc.system",
		"aws.s3.bucket",
		"exception.type",
		"faas.trigger",
		"feature_flag.key",
		"telemetry.sdk.language",
	}
)

func NormalizeClassification(m map[string]string, args ...[]*v11.KeyValue) map[string]string {
	_classification := DetermineClassification(args...)

	if m == nil || len(m) == 0 {
		return _classification
	}

	existingSpanType := m[TransactionType]
	existingSpanCategory := m[TransactionCategory]

	newSpanType := _classification[TransactionType]
	newSpanCategory := _classification[TransactionCategory]

	if existingSpanType == _webTransaction {
		newSpanType = _webTransaction
	}
	if newSpanCategory == "" {
		newSpanCategory = existingSpanCategory
	}

	return map[string]string{
		TransactionType:     newSpanType,
		TransactionCategory: newSpanCategory,
	}
}

// DetermineClassification returns a map of labels classifying the type of the span based on the predefined attributes in the span
func DetermineClassification(args ...[]*v11.KeyValue) map[string]string {
	spanType := _nonWebTransaction
	spanCategory := "unknown"

	attributes := map[string]string{}

	for _, attrs := range args {
		for _, attr := range attrs {
			key := strings.ToLower(strings.TrimSpace(attr.GetKey()))
			attributes[key] = attr.GetValue().GetStringValue()

			// set span type
			if strings.HasPrefix(key, "http.") ||
				strings.HasPrefix(key, "user_agent.") ||
				strings.HasPrefix(key, "rpc.") {
				spanType = _webTransaction
			}
		}
	}

	for _, c := range _categoryAttributes {
		if val, ok := attributes[c]; ok {
			spanCategory = val
			break
		}
	}

	return map[string]string{
		TransactionType:     spanType,
		TransactionCategory: spanCategory,
	}
}
