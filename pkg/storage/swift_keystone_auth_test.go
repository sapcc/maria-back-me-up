// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ncw/swift"
)

const catalogFixture = `{
  "token": {
    "expires_at": "2099-01-01T00:00:00Z",
    "catalog": [
      {
        "type": "object-store",
        "endpoints": [
          {"region": "region-1", "interface": "public",   "url": "https://swift.region-1.example/v1/AUTH_x"},
          {"region": "region-1", "interface": "internal", "url": "https://swift-int.region-1.example/v1/AUTH_x"},
          {"region": "region-2", "interface": "public",   "url": "https://swift.region-2.example/v1/AUTH_x"}
        ]
      },
      {
        "type": "object-store-ceph",
        "endpoints": [
          {"region": "region-1", "interface": "public",   "url": "https://ceph.region-1.example/swift/v1/AUTH_x"},
          {"region": "region-1", "interface": "internal", "url": "https://ceph-int.region-1.example/swift/v1/AUTH_x"}
        ]
      }
    ]
  }
}`

func newAuthFromFixture(t *testing.T, serviceType string) *keystoneV3Auth {
	t.Helper()
	a := &keystoneV3Auth{serviceType: serviceType}
	resp := &http.Response{
		Header: http.Header{},
		Body:   io.NopCloser(strings.NewReader(catalogFixture)),
	}
	if err := a.Response(resp); err != nil {
		t.Fatalf("Response: %v", err)
	}
	return a
}

func TestKeystoneV3StorageUrlForEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		serviceType string
		region      string
		endpoint    swift.EndpointType
		want        string
	}{
		{
			name:     "default service type, region matched, public",
			region:   "region-1",
			endpoint: swift.EndpointTypePublic,
			want:     "https://swift.region-1.example/v1/AUTH_x",
		},
		{
			name:     "default service type, region matched, internal",
			region:   "region-1",
			endpoint: swift.EndpointTypeInternal,
			want:     "https://swift-int.region-1.example/v1/AUTH_x",
		},
		{
			name:     "default service type, second region",
			region:   "region-2",
			endpoint: swift.EndpointTypePublic,
			want:     "https://swift.region-2.example/v1/AUTH_x",
		},
		{
			name:        "ceph override, public",
			serviceType: "object-store-ceph",
			region:      "region-1",
			endpoint:    swift.EndpointTypePublic,
			want:        "https://ceph.region-1.example/swift/v1/AUTH_x",
		},
		{
			name:        "ceph override, internal",
			serviceType: "object-store-ceph",
			region:      "region-1",
			endpoint:    swift.EndpointTypeInternal,
			want:        "https://ceph-int.region-1.example/swift/v1/AUTH_x",
		},
		{
			name:        "unknown service type returns empty",
			serviceType: "object-store-bogus",
			region:      "region-1",
			endpoint:    swift.EndpointTypePublic,
			want:        "",
		},
		{
			name:        "ceph override, region without that service returns empty",
			serviceType: "object-store-ceph",
			region:      "region-2",
			endpoint:    swift.EndpointTypePublic,
			want:        "",
		},
		{
			name:     "no region filter picks first matching endpoint",
			region:   "",
			endpoint: swift.EndpointTypePublic,
			want:     "https://swift.region-1.example/v1/AUTH_x",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthFromFixture(t, tc.serviceType)
			a.region = tc.region

			got := a.StorageUrlForEndpoint(tc.endpoint)
			if got != tc.want {
				t.Errorf("StorageUrlForEndpoint(%q) = %q, want %q", tc.endpoint, got, tc.want)
			}
		})
	}
}

func TestKeystoneV3StorageUrlDelegates(t *testing.T) {
	a := newAuthFromFixture(t, "")
	a.region = "region-1"

	if got, want := a.StorageUrl(false), "https://swift.region-1.example/v1/AUTH_x"; got != want {
		t.Errorf("StorageUrl(false) = %q, want %q", got, want)
	}
	if got, want := a.StorageUrl(true), "https://swift-int.region-1.example/v1/AUTH_x"; got != want {
		t.Errorf("StorageUrl(true) = %q, want %q", got, want)
	}
}
