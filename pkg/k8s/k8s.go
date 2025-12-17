// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"bytes"
	"context"

	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"time"

	"github.com/namsral/flag"
	"github.com/pingcap/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	// golint needed
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Database a k8s database representation
type Database struct {
	client *kubernetes.Clientset
	ns     string
	ys     *json.Serializer
}

// New creates a new database instance
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
		} else if kubeconfig == nil {
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

	return &Database{
		client: clientset,
		ns:     ns,
		ys: json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
			scheme.Scheme),
	}, err
}

// CreateDatabaseDeployment creates a k8s database deployment, based on a yaml file
func (m *Database) CreateDatabaseDeployment(name string, c config.DatabaseConfig) (deploy *appsv1.Deployment, err error) {
	var tpl string
	switch c.Type {
	case constants.MARIADB:
		tpl = constants.MARIADEPLOYMENT
	case constants.POSTGRES:
		tpl = constants.POSTGRESDEPLOYMENT
	}
	deploy, err = m.createDeployment(tpl, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			if err = m.ScaleDatabaseResources(name, 1); err != nil {
				return
			}
		}
		return
	}
	return
}

// CreateDatabaseService creates a k8s database service, based on a yaml file
func (m *Database) CreateDatabaseService(name string, c config.DatabaseConfig) (svc *v1.Service, err error) {
	var tpl string
	switch c.Type {
	case constants.MARIADB:
		tpl = constants.MARIASERIVCE
	case constants.POSTGRES:
		tpl = constants.POSTGRESSERIVCE
	}
	svc, err = m.createService(tpl, c)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return svc, nil
		}
		return
	}
	return
}

func (m *Database) createDeployment(p string, cfg any) (deploy *appsv1.Deployment, err error) {
	deploy = &appsv1.Deployment{}
	if err = m.unmarshalYamlFile(p, deploy, cfg); err != nil {
		return
	}

	return m.client.AppsV1().Deployments(m.ns).Create(context.Background(), deploy, metav1.CreateOptions{})
}

func (m *Database) createService(p string, cfg any) (svc *v1.Service, err error) {
	svc = &v1.Service{}
	if err = m.unmarshalYamlFile(p, svc, cfg); err != nil {
		return
	}
	return m.client.CoreV1().Services(m.ns).Create(context.Background(), svc, metav1.CreateOptions{})
}

// GetPodIP returns the pods actual ip based on as lable selector
func (m *Database) GetPodIP(labelSelector string) (ip string, err error) {
	p, err := m.client.CoreV1().Pods(m.ns).List(context.Background(), metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return ip, err
	}
	if len(p.Items) > 1 || len(p.Items) == 0 {
		return ip, errors.New("wrong number of database pods found")
	}
	return p.Items[0].Status.PodIP, nil
}

// CheckPodNotReady checks if a pod is in state NotReady
func (m *Database) CheckPodNotReady(labelSelector string) (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		p, err := m.client.CoreV1().Pods(m.ns).List(context.Background(), metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return false, err
		}
		if len(p.Items) > 1 || len(p.Items) == 0 {
			return false, errors.New("wrong number of database pods found")
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
	c := func(ctx context.Context) (bool, error) {
		return cf()
	}
	//Check for 1 minute if pod is in state  NotReady
	if err = wait.PollUntilContextTimeout(context.TODO(), 5*time.Second, 1*time.Minute, false, c); err != nil {
		return fmt.Errorf("wait Timed out for pod readiness: %s", err.Error())
	}

	return
}

// ScaleDatabaseResources scales k8s db resources
func (m *Database) ScaleDatabaseResources(name string, scale int32) (err error) {
	s, err := m.client.AppsV1().Deployments(m.ns).GetScale(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return
	}
	sc := *s
	sc.Spec.Replicas = scale
	_, err = m.client.AppsV1().Deployments(m.ns).UpdateScale(context.Background(), name, &sc, metav1.UpdateOptions{})
	return
}

func (m *Database) unmarshalYamlFile(p string, into runtime.Object, vl any) (err error) {
	var tpl bytes.Buffer
	s := json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
		scheme.Scheme)
	yamlBytes, err := os.ReadFile(p)
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
	_, _, err = s.Decode((tpl.Bytes()), &schema.GroupVersionKind{Version: "v1"}, into)
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
