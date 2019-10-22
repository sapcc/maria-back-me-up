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
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/pingcap/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/siddontang/go-mysql/client"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/api/apps/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Verifier struct {
	client *kubernetes.Clientset
	ns     string
	ys     *json.Serializer
}

type verifyMariaDbConfig struct {
	config.MariaDB
	Name string
}

func NewVerifier(n string) (v *Verifier, err error) {
	var config *rest.Config
	var kubeconfig *string
	config, err = rest.InClusterConfig()
	if err != nil {
		// use the current context in kubeconfig
		if home := homeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return v, err
	}

	return &Verifier{
		client: clientset,
		ns:     n,
		ys: json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
			scheme.Scheme),
	}, err
}

func (v *Verifier) createMariaDBDeployment(c config.MariaDB) (deploy *v1beta1.Deployment, err error) {
	deploy, err = v.createMariaDeployment("./templates/maria_deployment.yaml", c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = v.client.AppsV1beta1().Deployments(v.ns).Delete(c.Host, &metav1.DeleteOptions{}); err != nil {
				return
			}
			return v.createMariaDBDeployment(c)
		}
		return
	}
	return
}

func (v *Verifier) createMariaDBService(c config.MariaDB) (svc *v1.Service, err error) {
	svc, err = v.CreateMariaService("./templates/maria_svc.yaml", c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = v.client.CoreV1().Services(v.ns).Delete(c.Host, &metav1.DeleteOptions{}); err != nil {
				return
			}
			return v.createMariaDBService(c)
		}
		return
	}
	return
}

func (v *Verifier) deleteMariaDB(deploy *v1beta1.Deployment, svc *v1.Service) (err error) {
	if err = v.client.AppsV1beta1().Deployments(v.ns).Delete(deploy.Name, &metav1.DeleteOptions{}); err != nil {
		return
	}
	if err = v.client.CoreV1().Services(v.ns).Delete(svc.Name, &metav1.DeleteOptions{}); err != nil {
		return
	}
	return
}

func (v *Verifier) verifyBackup(cfg config.MariaDB) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := HealthCheck(cfg)
		if err != nil || !s.Ok {
			return false, nil
		}
		return true, nil
	})
	if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
		return
	}
	conn, _ := client.Connect(cfg.Host, cfg.User, cfg.Password, "dbName")
	if err = conn.Ping(); err != nil {
		return
	}

	//TODO: restore backup

	rs, err := conn.Execute("query")
	fmt.Println(rs)
	return
}

func (v *Verifier) createMariaDeployment(p string, cfg config.MariaDB) (deploy *v1beta1.Deployment, err error) {
	deploy = &v1beta1.Deployment{}
	if err = v.unmarshalYamlFile(p, deploy, cfg); err != nil {
		return
	}
	return v.client.AppsV1beta1().Deployments(v.ns).Create(deploy)
}

func (v *Verifier) CreateMariaService(p string, cfg config.MariaDB) (svc *v1.Service, err error) {
	svc = &v1.Service{}
	if err = v.unmarshalYamlFile(p, svc, cfg); err != nil {
		return
	}
	fmt.Println(svc.String())
	return v.client.CoreV1().Services(v.ns).Create(svc)
}

func (v *Verifier) unmarshalYamlFile(p string, into runtime.Object, vl config.MariaDB) (err error) {
	var tpl bytes.Buffer
	s := json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
		scheme.Scheme)
	yamlBytes, err := ioutil.ReadFile(p)
	if err != nil {
		return
	}
	t, err := template.New("k8s").Parse(string(yamlBytes))
	if err != nil {
		return
	}
	if err = t.Execute(&tpl, vl); err != nil {
		return
	}
	_, _, err = s.Decode([]byte(tpl.String()), &schema.GroupVersionKind{Version: "v1"}, into)
	if err != nil {
		return
	}
	return
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
