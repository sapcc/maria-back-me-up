/**
 * Copyright 2019 SAP SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"context"
	"net/http"

	"github.com/coreos/go-oidc"
	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"golang.org/x/oauth2"
)

var oauth2Config oauth2.Config
var provider *oidc.Provider
var idTokenVerifier *oidc.IDTokenVerifier

func init() {
	ctx := oidc.ClientContext(context.Background(), http.DefaultClient)
	provider, _ = oidc.NewProvider(ctx, "http://auth.mariabackup.qa-de-1.cloud.sap")
	idTokenVerifier = provider.Verifier(&oidc.Config{ClientID: "15c685ee35e84572b877"})

	oauth2Config = oauth2.Config{
		// client_id and client_secret of the client.
		ClientID:     "mariadb_backup",
		ClientSecret: "apie4eeX6hiC9ainieli",

		// The redirectURL.
		RedirectURL: "http://keystone.mariabackup.qa-de-1.cloud.sap/auth/callback",

		// Discovery returns the OAuth2 endpoints.
		Endpoint: provider.Endpoint(),

		// "openid" is a required scope for OpenID Connect flows.
		//
		// Other scopes, such as "groups" can be requested.
		Scopes: []string{"read:org", "read:user"},
	}

}

// handleRedirect is used to start an OAuth2 flow with the dex server.
func HandleRedirect(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		//authCodeURL = oauth2Config(scopes).AuthCodeURL(exampleAppState, oauth2.AccessTypeOffline)
		//tate := newState()
		http.Redirect(c.Response(), c.Request(), oauth2Config.AuthCodeURL("state"), http.StatusFound)
		return nil
	}
}

func HandleOAuth2Callback(m *backup.Manager) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		//state := c.Request().URL.Query().Get("state")
		ctx := c.Request().Context()
		// Verify state.

		oauth2Token, err := oauth2Config.Exchange(ctx, c.Request().URL.Query().Get("code"))
		if err != nil {
			// handle error
		}

		// Extract the ID Token from OAuth2 token.
		rawIDToken, ok := oauth2Token.Extra("id_token").(string)
		if !ok {
			// handle missing token
		}

		// Parse and verify ID Token payload.
		idToken, err := idTokenVerifier.Verify(ctx, rawIDToken)
		if err != nil {
			// handle error
		}

		// Extract custom claims.
		var claims struct {
			Email    string   `json:"email"`
			Verified bool     `json:"email_verified"`
			Groups   []string `json:"groups"`
		}
		if err := idToken.Claims(&claims); err != nil {
			// handle error
		}
		return
	}
}

func newState() {

}
