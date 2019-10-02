package discovery

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start() {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", s.health)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if true {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}
