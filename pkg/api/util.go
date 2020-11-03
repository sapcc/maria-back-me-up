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
	"fmt"
	"io/ioutil"

	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

const (
	verifyOkState          = "#ffc107" // orange
	verifyErrorState       = "#dc3545" // red
	verifyNotCompleteState = "#6c757d" // grey
	verifyCompleteState    = "#28a745" // green
)

func ReadFile(path string) (d string, err error) {
	fBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return d, fmt.Errorf("read file: %s", err.Error())
	}

	return string(fBytes), nil
}

func calcVerifyState(v storage.Verify, showError bool) string {
	verifyState := verifyNotCompleteState
	verifyError := "verification not completed..."
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

type backupSlice []storage.Backup

func (s backupSlice) Len() int {
	return len(s[0].IncList)
}

func (s backupSlice) Less(i, j int) bool {
	fmt.Println(s[0].IncList[i].LastModified)
	return (s[0].IncList[i].LastModified).After(s[0].IncList[j].LastModified)
}

func (s backupSlice) Swap(i, j int) {
	s[0].IncList[i], s[0].IncList[j] = s[0].IncList[j], s[0].IncList[i]
}
