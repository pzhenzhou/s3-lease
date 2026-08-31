package harness

import "testing"

func TestComposeServiceEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "IPv4 loopback with custom published port",
			endpoint: "http://127.0.0.1:18333",
			want:     "http://seaweedfs:8333",
		},
		{
			name:     "localhost with path",
			endpoint: "http://localhost:8333/base",
			want:     "http://seaweedfs:8333/base",
		},
		{
			name:     "remote endpoint unchanged",
			endpoint: "https://s3.example.com:9443/base",
			want:     "https://s3.example.com:9443/base",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := composeServiceEndpoint(test.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("composeServiceEndpoint(%q) = %q, want %q", test.endpoint, got, test.want)
			}
		})
	}
}

func TestContainerEndpointUsesHostGatewayAndPublishedPort(t *testing.T) {
	got, err := containerEndpoint("http://127.0.0.1:49152/proxy")
	if err != nil {
		t.Fatal(err)
	}
	if want := "http://host.docker.internal:49152/proxy"; got != want {
		t.Fatalf("containerEndpoint() = %q, want %q", got, want)
	}
}
