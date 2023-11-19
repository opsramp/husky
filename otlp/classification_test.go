package otlp

import (
	v11 "github.com/opsramp/husky/proto/otlp/common/v1"
	"reflect"
	"testing"
)

func TestNormalizeClassification(t *testing.T) {
	type args struct {
		m    map[string]string
		args [][]*v11.KeyValue
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "RPC Classification",
			args: args{
				m: map[string]string{
					"transaction.type":         "non-web",
					"transaction.category":     "Programming Language",
					"transaction.sub_category": "go",
					"language":                 "go",
				},
				args: [][]*v11.KeyValue{
					{
						{
							Key: "rpc.system",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "GRPC"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "web",
				"transaction.category":     "RPC Systems",
				"transaction.sub_category": "GRPC",
				"language":                 "go",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeClassification(tt.args.m, tt.args.args...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NormalizeClassification() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetermineClassification(t *testing.T) {
	type args struct {
		args [][]*v11.KeyValue
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "Programming Language Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "telemetry.sdk.language",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "go"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "non-web",
				"transaction.category":     "Programming Language",
				"transaction.sub_category": "go",
				"language":                 "go",
			},
		},
		{
			name: "Exceptions Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "exception.type",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "OutOfBounds"},
							},
						},
					},
					{
						{
							Key: "telemetry.sdk.language",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "java"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "non-web",
				"transaction.category":     "Exceptions",
				"transaction.sub_category": "OutOfBounds",
				"language":                 "java",
			},
		},
		{
			name: "RPC Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "rpc.system",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "GRPC"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "web",
				"transaction.category":     "RPC Systems",
				"transaction.sub_category": "GRPC",
				"language":                 "unknown",
			},
		},
		{
			name: "messaging Queues Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "messaging.system",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "kafka"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "non-web",
				"transaction.category":     "Messaging queues",
				"transaction.sub_category": "kafka",
				"language":                 "unknown",
			},
		},
		{
			name: "HTTP Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "http.request.method",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "test"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "web",
				"transaction.category":     "HTTP",
				"transaction.sub_category": "test",
				"language":                 "unknown",
			},
		},
		{
			name: "DB Classification",
			args: args{
				args: [][]*v11.KeyValue{
					{
						{
							Key: "db.system",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "mysql"},
							},
						},
						{
							Key: "telemetry.sdk.language",
							Value: &v11.AnyValue{
								Value: &v11.AnyValue_StringValue{StringValue: "c"},
							},
						},
					},
				},
			},
			want: map[string]string{
				"transaction.type":         "non-web",
				"transaction.category":     "Databases",
				"transaction.sub_category": "mysql",
				"language":                 "c",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetermineClassification(tt.args.args...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DetermineClassification() = %v, want %v", got, tt.want)
			}
		})
	}
}
