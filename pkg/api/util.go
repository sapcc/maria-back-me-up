// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

const (
	verifyOkState          = "#ffc107" // orange
	verifyErrorState       = "#dc3545" // red
	verifyNotCompleteState = "#6c757d" // grey
	verifyCompleteState    = "#28a745" // green
)

func calcVerifyState(v *storage.Verify, showError bool) string {
	verifyState := verifyNotCompleteState
	verifyError := "verification not completed..."
	if v == nil {
		if showError {
			return verifyError
		}
		return verifyState
	}
	if v.VerifyRestore == 1 && v.VerifyDiff == 1 {
		verifyState = verifyOkState
		if v.VerifyError != "" {
			verifyError = v.VerifyError
		} else {
			verifyError = "mySQL restore and diff successful! Table checksum was not executed yet."
		}
	}
	if v.VerifyChecksum == 1 {
		verifyState = verifyCompleteState
		verifyError = "mySQL checksum successful"
	}
	if v.VerifyRestore == 0 || v.VerifyDiff == 0 {
		verifyState = verifyErrorState
		if v.VerifyError != "" {
			verifyError = v.VerifyError
		}
	}
	if showError {
		return verifyError
	}
	return verifyState
}

type incBackupSlice []storage.Backup

func (s incBackupSlice) Len() int {
	return len(s[0].IncList)
}

func (s incBackupSlice) Less(i, j int) bool {
	return (s[0].IncList[i].LastModified).After(s[0].IncList[j].LastModified)
}

func (s incBackupSlice) Swap(i, j int) {
	s[0].IncList[i], s[0].IncList[j] = s[0].IncList[j], s[0].IncList[i]
}

type backupSlice []storage.Backup

func (s backupSlice) Len() int {
	return len(s)
}

func (s backupSlice) Less(i, j int) bool {
	return (s[i].Time).After(s[j].Time)
}

func (s backupSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
