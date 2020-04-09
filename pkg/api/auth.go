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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/coreos/go-oidc"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"golang.org/x/oauth2"
)

var (
	oauth2Config         oauth2.Config
	provider             *oidc.Provider
	idTokenVerifier      *oidc.IDTokenVerifier
	oauthStateCookieName = "oauth_state"
	sessionCookieName    = "oauth_session"
	store                *sessions.CookieStore
)

func InitAPI(m *backup.Manager, opts config.Options) {
	ctx := oidc.ClientContext(context.Background(), http.DefaultClient)
	key := make([]byte, 64)

	_, err := rand.Read(key)
	store = sessions.NewCookieStore([]byte(opts.CookieSecret)) //TODO: load via env vars
	store.Options = &sessions.Options{
		Path: "/",
		//MaxAge:   60,
		HttpOnly: true,
	}
	provider, err := oidc.NewProvider(ctx, m.GetConfig().OAuth.ProviderURL)
	if err != nil {
		return
	}
	idTokenVerifier = provider.Verifier(&oidc.Config{ClientID: opts.ClientID})
	if idTokenVerifier == nil {
		log.Fatal("cannot init IDTokenVerifier")
	}

	oauth2Config = oauth2.Config{
		ClientID:     opts.ClientID,
		ClientSecret: opts.ClientSecret,

		RedirectURL: m.GetConfig().OAuth.RedirectURL + "/auth/callback",
		Endpoint:    provider.Endpoint(),

		Scopes: []string{oidc.ScopeOpenID, "groups", "email"},
	}

}

// Oauth middleware is used to start an OAuth2 flow with the dex server.
func Oauth(enabled bool, opts config.Options) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			if !enabled {
				return next(c)
			}
			if ok := checkAuthenticated(c.Request()); ok {
				return next(c)
			}

			state, _ := genStateString()
			hashedState := hashStatecode(state, opts.ClientSecret)
			writeCookie(c.Response(), hashedState, 1)
			if err := updateSessionStore(c.Response(), c.Request(), "", "claims.Email", c.Request().URL.String()); err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "Error OAuth", err)
			}
			return c.Redirect(http.StatusTemporaryRedirect, oauth2Config.AuthCodeURL(state, oauth2.AccessTypeOnline))
		}
	}
}

func checkAuthenticated(r *http.Request) bool {
	session, err := store.Get(r, sessionCookieName)
	if err != nil {
		return false
	}
	token, ok := session.Values["token"].(string)
	if !ok {
		return false
	}
	if _, err := idTokenVerifier.Verify(r.Context(), token); err != nil {
		return false
	}
	return true
}

func updateSessionStore(w http.ResponseWriter, r *http.Request, token string, user, url string) error {
	session, err := store.Get(r, sessionCookieName)
	if err != nil {
		return err
	}
	session.Values["token"] = token
	session.Values["user"] = user
	session.Values["url"] = url
	return session.Save(r, w)
}

func HandleOAuth2Callback(opts config.Options) echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		ctx := c.Request().Context()
		cookieState, err := c.Cookie(oauthStateCookieName)
		if err != nil {
			return echo.NewHTTPError(500, "OAuth Login failed")
		}
		queryState := hashStatecode(c.Request().URL.Query().Get("state"), opts.ClientSecret)

		if cookieState.Value != queryState {
			return echo.NewHTTPError(500, "OAuth Login: state mismatch")
		}
		oauth2Token, err := oauth2Config.Exchange(ctx, c.Request().URL.Query().Get("code"))
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		rawIDToken, ok := oauth2Token.Extra("id_token").(string)
		if !ok {
			fmt.Println(err.Error())
			return
		}

		idToken, err := idTokenVerifier.Verify(ctx, rawIDToken)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		// Extract custom claims.
		var claims struct {
			Email    string   `json:"email"`
			Verified bool     `json:"email_verified"`
			Groups   []string `json:"groups"`
		}

		if err = idToken.Claims(&claims); err != nil {
			fmt.Println(err.Error())
			return
		}

		session, err := store.Get(c.Request(), sessionCookieName)
		url := session.Values["url"].(string)

		updateSessionStore(c.Response(), c.Request(), rawIDToken, claims.Email, "")

		return c.Redirect(http.StatusTemporaryRedirect, url)
	}
}

func writeCookie(w http.ResponseWriter, value string, sameSite http.SameSite) {
	cookie := http.Cookie{
		Name:     oauthStateCookieName,
		MaxAge:   60,
		Value:    value,
		HttpOnly: true,
	}
	if sameSite != http.SameSiteDefaultMode {
		cookie.SameSite = sameSite
	}
	http.SetCookie(w, &cookie)
}

func hashStatecode(code, seed string) string {
	hashBytes := sha256.Sum256([]byte(code + seed))
	return hex.EncodeToString(hashBytes[:])
}

func genStateString() (string, error) {
	rnd := make([]byte, 32)
	if _, err := rand.Read(rnd); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(rnd), nil
}
