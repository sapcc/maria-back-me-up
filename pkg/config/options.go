// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package config

// Options passed via cmd line
type Options struct {
	LogLevel       string
	WriteTo        string
	ConfigmapName  string
	Version        string
	ConfigFilePath string
	NameSpace      string
	Region         string
	ClientID       string
	ClientSecret   string
	CookieSecret   string
}
