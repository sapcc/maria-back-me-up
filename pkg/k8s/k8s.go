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

package k8s

import (
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/apps/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var deletePolicy = metav1.DeletePropagationForeground

var k8sDatabase = map[string]string{
	constants.MARIADB: constants.MARIADEPLOYMENT,
}

type Database struct {
	client *kubernetes.Clientset
	ns     string
	ys     *json.Serializer
}

func New(ns string) (m *Database, err error) {
	var config *rest.Config
	var kubeconfig *string
	config, err = rest.InClusterConfig()
	if err != nil {
		// use the current context in kubeconfig
		flag.VisitAll(func(f *flag.Flag) {
			if f.Name == "kubeconfig" {
				v := f.Value.String()
				kubeconfig = &v
			}
		})
		if home := homeDir(); home != "" && kubeconfig == nil {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else if &kubeconfig == nil {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		} else {

		}
		flag.Parse()
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return m, err
	}

	return &Database{
		client: clientset,
		ns:     ns,
		ys: json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
			scheme.Scheme),
	}, err
}

func (m *Database) CreateDatabaseDeployment(name string, c config.DatabaseConfig) (deploy *appsv1.Deployment, err error) {
	var tpl string
	if c.Type == constants.MARIADB {
		tpl = constants.MARIADEPLOYMENT
	} else if c.Type == constants.POSTGRES {
		tpl = constants.POSTGRESDEPLOYMENT
	}
	deploy, err = m.createDeployment(tpl, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = m.client.AppsV1().Deployments(m.ns).Delete(name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return
			}
			time.Sleep(10 * time.Second)
			return m.CreateDatabaseDeployment(name, c)
		}
		return
	}
	return
}

func (m *Database) CreateDatabaseService(name string, c config.DatabaseConfig) (svc *v1.Service, err error) {
	var tpl string
	if c.Type == constants.MARIADB {
		tpl = constants.MARIASERIVCE
	} else if c.Type == constants.POSTGRES {
		tpl = constants.POSTGRESSERIVCE
	}
	svc, err = m.createService(tpl, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = m.client.CoreV1().Services(m.ns).Delete(name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return
			}
			return m.CreateDatabaseService(name, c)
		}
		return
	}
	return
}

func (m *Database) createDeployment(p string, cfg interface{}) (deploy *appsv1.Deployment, err error) {
	deploy = &appsv1.Deployment{}
	if err = m.unmarshalYamlFile(p, deploy, cfg); err != nil {
		return
	}
	return m.client.AppsV1().Deployments(m.ns).Create(deploy)
}

func (m *Database) createService(p string, cfg interface{}) (svc *v1.Service, err error) {
	svc = &v1.Service{}
	if err = m.unmarshalYamlFile(p, svc, cfg); err != nil {
		return
	}
	return m.client.CoreV1().Services(m.ns).Create(svc)
}

func (m *Database) GetPodIP(labelSelector string) (ip string, err error) {
	p, err := m.client.CoreV1().Pods(m.ns).List(metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return ip, err
	}
	if len(p.Items) > 1 || len(p.Items) == 0 {
		return ip, fmt.Errorf("wrong number of database pods found")
	}
	return p.Items[0].Status.PodIP, nil
}

func (m *Database) CheckPodNotReady(labelSelector string) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		p, err := m.client.CoreV1().Pods(m.ns).List(metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return false, err
		}
		if len(p.Items) > 1 || len(p.Items) == 0 {
			return false, fmt.Errorf("wrong number of database pods found")
		}
		for c := range p.Items[0].Status.Conditions {
			if p.Items[0].Status.Conditions[c].Type == v1.PodReady {
				if p.Items[0].Status.Conditions[c].Status == v1.ConditionFalse {
					return true, nil
				}
			}
		}
		return false, nil
	})
	//Check for 1 minute if pod is in state  NotReady
	if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
		return fmt.Errorf("Wait Timed out for pod readiness: %s", err.Error())
	}

	return
}

func (m *Database) DeleteDatabaseResources(deploy *v1beta1.Deployment, svc *v1.Service) (err error) {
	if err = m.client.AppsV1beta1().Deployments(m.ns).Delete(deploy.Name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		return
	}
	if err = m.client.CoreV1().Services(m.ns).Delete(svc.Name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		return
	}
	return
}

func (m *Database) unmarshalYamlFile(p string, into runtime.Object, vl interface{}) (err error) {
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
