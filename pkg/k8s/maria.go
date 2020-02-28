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

type Maria struct {
	client *kubernetes.Clientset
	ns     string
	ys     *json.Serializer
}

func NewMaria(ns string) (m *Maria, err error) {
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
		return m, err
	}

	return &Maria{
		client: clientset,
		ns:     ns,
		ys: json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
			scheme.Scheme),
	}, err
}

func (m *Maria) CreateMariaDeployment(c config.MariaDB) (deploy *v1beta1.Deployment, err error) {
	deploy, err = m.createDeployment(constants.MARIADEPLOYMENT, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = m.client.AppsV1beta1().Deployments(m.ns).Delete(c.Host, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return
			}
			time.Sleep(10 * time.Second)
			return m.CreateMariaDeployment(c)
		}
		return
	}
	return
}

func (m *Maria) CreateMariaService(c config.MariaDB) (svc *v1.Service, err error) {
	svc, err = m.createService(constants.MARIASERIVCE, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = m.client.CoreV1().Services(m.ns).Delete(c.Host, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				return
			}
			return m.CreateMariaService(c)
		}
		return
	}
	return
}

func (m *Maria) createDeployment(p string, cfg interface{}) (deploy *v1beta1.Deployment, err error) {
	deploy = &v1beta1.Deployment{}
	if err = m.unmarshalYamlFile(p, deploy, cfg); err != nil {
		return
	}
	fmt.Println("===============================", m.ns)
	return m.client.AppsV1beta1().Deployments(m.ns).Create(deploy)
}

func (m *Maria) createService(p string, cfg interface{}) (svc *v1.Service, err error) {
	svc = &v1.Service{}
	if err = m.unmarshalYamlFile(p, svc, cfg); err != nil {
		return
	}
	return m.client.CoreV1().Services(m.ns).Create(svc)
}

func (m *Maria) CheckPodNotReady() (err error) {
	name := os.Getenv("HOSTNAME")
	if name == "" {
		return fmt.Errorf("No env HOSTNAME found")
	}
	cf := wait.ConditionFunc(func() (bool, error) {
		p, err := m.client.CoreV1().Pods(m.ns).Get(name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for c := range p.Status.Conditions {
			if p.Status.Conditions[c].Type == v1.PodReady {
				if p.Status.Conditions[c].Status == v1.ConditionFalse {
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

func (m *Maria) DeleteMariaResources(deploy *v1beta1.Deployment, svc *v1.Service) (err error) {
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

func (m *Maria) unmarshalYamlFile(p string, into runtime.Object, vl interface{}) (err error) {
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
