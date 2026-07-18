package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1/controlv1connect"
	"github.com/anmho/inference-data-plane/controlplane/internal/catalog"
	"github.com/anmho/inference-data-plane/controlplane/internal/config"
	"github.com/anmho/inference-data-plane/controlplane/internal/hostagent"
)

func main() {
	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		configPath = "config/host-agent.local.yaml"
	}
	cfg, err := config.Load[config.HostAgent](configPath)
	if err != nil {
		log.Fatal(err)
	}
	models, err := catalog.Load(cfg.CatalogGlob)
	if err != nil {
		log.Fatal(err)
	}
	agent := hostagent.New(cfg, models)
	path, handler := controlv1connect.NewMlxHostAgentServiceHandler(agent)
	mux := http.NewServeMux()
	mux.Handle(path, hostagent.Authorize(cfg.AuthToken, handler))
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok"))
	})
	server := &http.Server{Addr: cfg.BindAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopped
		agent.StopAll()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("MLX host agent listening on %s", cfg.BindAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
