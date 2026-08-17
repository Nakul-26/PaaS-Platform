package cmd

import (
	"testing"

	"platform/apps/cli/internal/apiclient"
)

func TestParsePortFlags(t *testing.T) {
	tests := []struct {
		name    string
		raw     []string
		want    []apiclient.PortSpec
		wantErr bool
	}{
		{name: "empty", raw: nil, want: []apiclient.PortSpec{}},
		{
			name: "container port only, ephemeral host port",
			raw:  []string{"80"},
			want: []apiclient.PortSpec{{ContainerPort: 80, Protocol: "tcp"}},
		},
		{
			name: "explicit host port",
			raw:  []string{"80:8080"},
			want: []apiclient.PortSpec{{ContainerPort: 80, HostPort: 8080, Protocol: "tcp"}},
		},
		{
			name: "multiple bindings",
			raw:  []string{"80:8080", "443"},
			want: []apiclient.PortSpec{
				{ContainerPort: 80, HostPort: 8080, Protocol: "tcp"},
				{ContainerPort: 443, Protocol: "tcp"},
			},
		},
		{name: "non-numeric container port", raw: []string{"abc"}, wantErr: true},
		{name: "non-numeric host port", raw: []string{"80:abc"}, wantErr: true},
		{name: "trailing colon treated as no host port", raw: []string{"80:"}, want: []apiclient.PortSpec{{ContainerPort: 80, Protocol: "tcp"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePortFlags(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}
