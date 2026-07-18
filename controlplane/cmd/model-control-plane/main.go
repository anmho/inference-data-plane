package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1/controlv1connect"
	"github.com/anmho/inference-data-plane/controlplane/internal/catalog"
	"github.com/anmho/inference-data-plane/controlplane/internal/config"
	"github.com/anmho/inference-data-plane/controlplane/internal/connectapi"
	"github.com/anmho/inference-data-plane/controlplane/internal/lifecycle"
	"go.temporal.io/sdk/client"
)

func main() {
	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		configPath = "config/control-plane.local.yaml"
	}
	cfg, err := config.Load[config.ControlPlane](configPath)
	if err != nil {
		log.Fatal(err)
	}
	models, err := catalog.Load(cfg.CatalogGlob)
	if err != nil {
		log.Fatal(err)
	}
	activities := lifecycle.NewActivities(cfg.HostAgentURL, cfg.HostAgentToken)
	var temporalClient client.Client
	var stopWorker func()
	for attempt := 1; attempt <= 60; attempt++ {
		temporalClient, stopWorker, err = lifecycle.StartWorker(cfg, activities)
		if err == nil {
			break
		}
		log.Printf("Temporal unavailable (attempt %d/60): %v", attempt, err)
		time.Sleep(2 * time.Second)
	}
	if temporalClient == nil {
		log.Fatalf("Temporal remained unavailable: %v", err)
	}
	defer stopWorker()

	path, handler := controlv1connect.NewModelControlServiceHandler(
		connectapi.NewModelControl(models, temporalClient, cfg),
	)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok"))
	})
	server := &http.Server{Addr: cfg.BindAddr, Handler: mux}
	go func() {
		log.Printf("model control plane listening on %s", cfg.BindAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}
