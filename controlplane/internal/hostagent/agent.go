package hostagent

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"connectrpc.com/connect"
	controlv1 "github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1"
	"github.com/anmho/inference-data-plane/controlplane/internal/catalog"
	"github.com/anmho/inference-data-plane/controlplane/internal/config"
)

type process struct {
	command  *exec.Cmd
	state    string
	modelID  string
	endpoint string
}

type Agent struct {
	mu      sync.Mutex
	config  config.HostAgent
	catalog *catalog.Catalog
	models  map[string]*process
	http    *http.Client
}

func New(cfg config.HostAgent, models *catalog.Catalog) *Agent {
	return &Agent{
		config:  cfg,
		catalog: models,
		models:  make(map[string]*process),
		http:    &http.Client{Timeout: time.Second},
	}
}

func (a *Agent) EnsureModel(ctx context.Context, request *connect.Request[controlv1.EnsureModelRequest]) (*connect.Response[controlv1.HostModelStatus], error) {
	entry, ok := a.catalog.Get(request.Msg.ModelId)
	if !ok || entry.Model.LocalRuntime != request.Msg.ModelId {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("model %q is not allowed by the catalog", request.Msg.ModelId))
	}
	a.mu.Lock()
	if current := a.models[request.Msg.ModelId]; current != nil && current.command.ProcessState == nil {
		endpoint := current.endpoint
		a.mu.Unlock()
		if a.endpointReady(ctx, endpoint) {
			a.mu.Lock()
			if a.models[request.Msg.ModelId] != current {
				a.mu.Unlock()
				return nil, connect.NewError(connect.CodeAborted, fmt.Errorf("owned MLX process changed while checking readiness"))
			}
			current.state = "ready"
			response := status(current, "already running")
			a.mu.Unlock()
			return connect.NewResponse(response), nil
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("owned MLX process is running but model endpoint is not ready"))
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", a.config.MLX.Port)
	if a.endpointReady(ctx, endpoint) {
		a.mu.Unlock()
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("port %d is owned by an untracked process", a.config.MLX.Port))
	}
	if err := os.MkdirAll(a.config.MLX.RunDir, 0o755); err != nil {
		a.mu.Unlock()
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	logPath := filepath.Join(a.config.MLX.RunDir, "mlx-lm.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		a.mu.Unlock()
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	command := exec.Command(
		a.config.MLX.Executable,
		"--model", entry.Model.LocalRuntime,
		"--host", a.config.MLX.Host,
		"--port", fmt.Sprint(a.config.MLX.Port),
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		logFile.Close()
		a.mu.Unlock()
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("start MLX model: %w", err))
	}
	owned := &process{command: command, state: "starting", modelID: request.Msg.ModelId, endpoint: endpoint}
	a.models[request.Msg.ModelId] = owned
	a.mu.Unlock()
	go func() {
		_ = command.Wait()
		_ = logFile.Close()
	}()

	deadline := time.Now().Add(time.Duration(a.config.MLX.StartupTimeoutSeconds) * time.Second)
	for time.Now().Before(deadline) {
		if a.endpointReady(ctx, endpoint) {
			a.mu.Lock()
			if a.models[request.Msg.ModelId] == owned {
				owned.state = "ready"
			}
			a.mu.Unlock()
			return connect.NewResponse(status(owned, "model ready")), nil
		}
		select {
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	a.stopOwned(request.Msg.ModelId)
	return nil, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("MLX model did not become ready before timeout"))
}

func (a *Agent) GetModelStatus(_ context.Context, request *connect.Request[controlv1.GetModelRequest]) (*connect.Response[controlv1.HostModelStatus], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.models[request.Msg.ModelId]
	if current == nil {
		return connect.NewResponse(&controlv1.HostModelStatus{ModelId: request.Msg.ModelId, State: "stopped"}), nil
	}
	if current.command.ProcessState != nil {
		current.state = "stopped"
	}
	return connect.NewResponse(status(current, "")), nil
}

func (a *Agent) DrainModel(_ context.Context, request *connect.Request[controlv1.ModelLifecycleRequest]) (*connect.Response[controlv1.HostModelStatus], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.models[request.Msg.ModelId]
	if current == nil {
		return connect.NewResponse(&controlv1.HostModelStatus{ModelId: request.Msg.ModelId, State: "stopped"}), nil
	}
	current.state = "draining"
	return connect.NewResponse(status(current, "new requests should no longer be routed to this model")), nil
}

func (a *Agent) StopModel(_ context.Context, request *connect.Request[controlv1.ModelLifecycleRequest]) (*connect.Response[controlv1.HostModelStatus], error) {
	if err := a.stopOwned(request.Msg.ModelId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&controlv1.HostModelStatus{ModelId: request.Msg.ModelId, State: "stopped"}), nil
}

func (a *Agent) StopAll() {
	a.mu.Lock()
	modelIDs := make([]string, 0, len(a.models))
	for modelID := range a.models {
		modelIDs = append(modelIDs, modelID)
	}
	a.mu.Unlock()
	for _, modelID := range modelIDs {
		_ = a.stopOwned(modelID)
	}
}

func (a *Agent) stopOwned(modelID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.models[modelID]
	if current == nil {
		return nil
	}
	delete(a.models, modelID)
	if current.command.Process != nil && current.command.ProcessState == nil {
		if err := current.command.Process.Signal(os.Interrupt); err != nil {
			_ = current.command.Process.Kill()
			return fmt.Errorf("stop owned MLX process: %w", err)
		}
	}
	return nil
}

func (a *Agent) endpointReady(ctx context.Context, endpoint string) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v1/models", nil)
	if err != nil {
		return false
	}
	response, err := a.http.Do(request)
	if err != nil {
		return false
	}
	response.Body.Close()
	return response.StatusCode == http.StatusOK
}

func status(current *process, message string) *controlv1.HostModelStatus {
	pid := int64(0)
	if current.command.Process != nil {
		pid = int64(current.command.Process.Pid)
	}
	return &controlv1.HostModelStatus{
		ModelId:  current.modelID,
		State:    current.state,
		Pid:      pid,
		Endpoint: current.endpoint,
		Message:  message,
	}
}

func Authorize(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request)
	})
}
