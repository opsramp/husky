package otlp

import (
	v11 "github.com/opsramp/husky/proto/otlp/common/v1"
	"strings"
)

const (
	TransactionType        = "transaction.type"
	_webTransaction        = "web"
	_nonWebTransaction     = "non-web"
	TransactionCategory    = "transaction.category"
	TransactionSubCategory = "transaction.sub_category"
)

var (
	_categoryAttributes = [][2]string{
		{"http.request.method", "HTTP"},
		{"db.system", "Databases"},
		{"messaging.system", "Messaging queues"},
		{"rpc.system", "RPC Systems"},
		{"aws.s3.bucket", "Object Store"},
		{"exception.type", "Exceptions"},
		{"faas.trigger", "FAAS (Function as a service)"},
		{"feature_flag.key", "Feature Flag"},
		{"telemetry.sdk.language", "Programming Language"},
	}
)

func NormalizeClassification(m map[string]string, args ...[]*v11.KeyValue) map[string]string {
	_classification := DetermineClassification(args...)

	if len(m) == 0 {
		return _classification
	}

	existingSpanType := m[TransactionType]
	existingSpanCategory := m[TransactionCategory]
	existingSpanSubCategory := m[TransactionSubCategory]

	newSpanType := _classification[TransactionType]
	newSpanCategory := _classification[TransactionCategory]
	newSpanSubCategory := _classification[TransactionSubCategory]

	if existingSpanType == _webTransaction {
		newSpanType = _webTransaction
	}
	if newSpanCategory == "" {
		newSpanCategory = existingSpanCategory
		newSpanSubCategory = existingSpanSubCategory
	}

	return map[string]string{
		TransactionType:        newSpanType,
		TransactionCategory:    newSpanCategory,
		TransactionSubCategory: newSpanSubCategory,
	}
}

// DetermineClassification returns a map of labels classifying the type of the span based on the predefined attributes in the span
func DetermineClassification(args ...[]*v11.KeyValue) map[string]string {
	spanType := _nonWebTransaction
	spanCategory := "unknown"
	spanSubCategory := "unknown"

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
		if val, ok := attributes[c[0]]; ok {
			spanSubCategory = val
			spanCategory = c[1]
			break
		}
	}

	return map[string]string{
		TransactionType:        spanType,
		TransactionCategory:    spanCategory,
		TransactionSubCategory: spanSubCategory,
	}
}
