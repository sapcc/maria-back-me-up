// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ncw/swift"
)

// keystoneV3Auth is a swift.Authenticator that resolves the Swift endpoint
// from the Keystone catalog using a configurable service type. Needed
// because ncw/swift hardcodes Type == "object-store" and reapplies that
// lookup on every re-auth, so the override has to live inside the
// Authenticator rather than as a one-shot StorageUrl patch after
// Authenticate().
type keystoneV3Auth struct {
	serviceType string

	region  string
	headers http.Header
	resp    struct {
		Token struct {
			ExpiresAt string `json:"expires_at"`
			Catalog   []struct {
				Type      string `json:"type"`
				Endpoints []struct {
					Region    string             `json:"region"`
					URL       string             `json:"url"`
					Interface swift.EndpointType `json:"interface"`
				} `json:"endpoints"`
			} `json:"catalog"`
		} `json:"token"`
	}
}

type v3PasswordRequest struct {
	Auth v3PasswordAuth `json:"auth"`
}

type v3PasswordAuth struct {
	Identity v3PasswordIdentity `json:"identity"`
	Scope    v3PasswordScope    `json:"scope"`
}

type v3PasswordIdentity struct {
	Methods  []string             `json:"methods"`
	Password v3PasswordIdentityPw `json:"password"`
}

type v3PasswordIdentityPw struct {
	User v3PasswordUser `json:"user"`
}

type v3PasswordUser struct {
	Name     string              `json:"name"`
	Password string              `json:"password"`
	Domain   v3PasswordRefByName `json:"domain"`
}

type v3PasswordScope struct {
	Project v3PasswordProject `json:"project"`
}

type v3PasswordProject struct {
	Name   string              `json:"name"`
	Domain v3PasswordRefByName `json:"domain"`
}

type v3PasswordRefByName struct {
	Name string `json:"name"`
}

func (a *keystoneV3Auth) Request(c *swift.Connection) (*http.Request, error) {
	a.region = c.Region

	body := v3PasswordRequest{
		Auth: v3PasswordAuth{
			Identity: v3PasswordIdentity{
				Methods: []string{"password"},
				Password: v3PasswordIdentityPw{
					User: v3PasswordUser{
						Name:     c.UserName,
						Password: c.ApiKey,
						Domain:   v3PasswordRefByName{Name: c.Domain},
					},
				},
			},
			Scope: v3PasswordScope{
				Project: v3PasswordProject{
					Name:   c.Tenant,
					Domain: v3PasswordRefByName{Name: c.TenantDomain},
				},
			},
		},
	}

	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	url := c.AuthUrl
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	url += "auth/tokens"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	return req, nil
}

func (a *keystoneV3Auth) Response(resp *http.Response) error {
	a.headers = resp.Header

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read keystone v3 response: %w", err)
	}
	if err := json.Unmarshal(body, &a.resp); err != nil {
		return fmt.Errorf("parse keystone v3 response: %w", err)
	}
	return nil
}

func (a *keystoneV3Auth) StorageUrl(internal bool) string {
	endpointType := swift.EndpointTypePublic
	if internal {
		endpointType = swift.EndpointTypeInternal
	}
	return a.StorageUrlForEndpoint(endpointType)
}

func (a *keystoneV3Auth) StorageUrlForEndpoint(endpointType swift.EndpointType) string {
	wantType := a.serviceType
	if wantType == "" {
		wantType = "object-store"
	}
	for _, svc := range a.resp.Token.Catalog {
		if svc.Type != wantType {
			continue
		}
		for _, ep := range svc.Endpoints {
			if ep.Interface != endpointType {
				continue
			}
			if a.region != "" && ep.Region != a.region {
				continue
			}
			return ep.URL
		}
	}
	return ""
}

func (a *keystoneV3Auth) Token() string {
	if a.headers == nil {
		return ""
	}
	return a.headers.Get("X-Subject-Token")
}

func (a *keystoneV3Auth) CdnUrl() string {
	return ""
}

func (a *keystoneV3Auth) Expires() time.Time {
	t, err := time.Parse(time.RFC3339, a.resp.Token.ExpiresAt)
	if err != nil {
		return time.Time{}
	}
	return t
}
