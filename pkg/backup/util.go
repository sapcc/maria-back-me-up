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

package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/yaml.v2"
)

func checkBackupDirExistsAndCreate(d string) (p string, err error) {
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, os.ModePerm)
		return d, err
	}
	return
}

func getMyDumpBinlog(p string) (mp mysql.Position, err error) {
	meta := metadata{}
	yamlBytes, err := ioutil.ReadFile(path.Join(p, "/metadata"))
	if err != nil {
		return mp, fmt.Errorf("read config file: %s", err.Error())
	}
	//turn string to valid yaml
	yamlCorrect := strings.ReplaceAll(string(yamlBytes), "\t", "  ")
	r, _ := regexp.Compile("([a-zA-Z])[\\:]([^\\s])")
	err = yaml.Unmarshal([]byte(r.ReplaceAllString(yamlCorrect, `$1: $2`)), &meta)
	if err != nil {
		return mp, fmt.Errorf("parse config file: %s", err.Error())
	}
	mp.Name = meta.Status.Log
	mp.Pos = meta.Status.Pos

	return
}

func getMysqlDumpBinlog(s string) (mp mysql.Position, err error) {
	var rex = regexp.MustCompile("(\\w+)=([^;,]*)")
	data := rex.FindAllStringSubmatch(s, -1)
	res := make(map[string]string)
	for _, kv := range data {
		k := kv[1]
		v := kv[2]
		res[k] = v
	}
	pos, err := strconv.ParseInt(res["MASTER_LOG_POS"], 10, 32)
	mp.Name = res["MASTER_LOG_FILE"]
	mp.Pos = uint32(pos)
	return
}

func compareChecksums(cs map[string]int64, with map[string]int64) error {
	for k, v := range cs {
		if with[k] != v {
			return fmt.Errorf("Backup verify table checksum mismatch for table %s", k)
		}
	}
	return nil
}
