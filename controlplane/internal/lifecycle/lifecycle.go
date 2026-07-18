package lifecycle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"
	controlv1 "github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1"
	"github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1/controlv1connect"
	"github.com/anmho/inference-data-plane/controlplane/internal/config"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type ModelInput struct {
	ModelID       string
	ModelEndpoint string
	KServeName    string
	Namespace     string
}

type ModelResult struct {
	ModelID  string
	State    string
	Endpoint string
}

type Activities struct {
	hostAgent controlv1connect.MlxHostAgentServiceClient
	token     string
}

func NewActivities(hostAgentURL, token string) *Activities {
	return &Activities{
		hostAgent: controlv1connect.NewMlxHostAgentServiceClient(http.DefaultClient, hostAgentURL),
		token:     token,
	}
}

func LoadModelWorkflow(ctx workflow.Context, input ModelInput) (ModelResult, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 1.5,
			MaximumInterval:    15 * time.Second,
			MaximumAttempts:    30,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	var status controlv1.HostModelStatus
	if err := workflow.ExecuteActivity(ctx, "EnsureHostModel", input).Get(ctx, &status); err != nil {
		return ModelResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, "WaitKServeReady", input).Get(ctx, nil); err != nil {
		return ModelResult{}, err
	}
	return ModelResult{ModelID: input.ModelID, State: "ready", Endpoint: status.Endpoint}, nil
}

func DrainModelWorkflow(ctx workflow.Context, input ModelInput) (ModelResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var status controlv1.HostModelStatus
	if err := workflow.ExecuteActivity(ctx, "DrainHostModel", input).Get(ctx, &status); err != nil {
		return ModelResult{}, err
	}
	return ModelResult{ModelID: input.ModelID, State: status.State, Endpoint: status.Endpoint}, nil
}

func UnloadModelWorkflow(ctx workflow.Context, input ModelInput) (ModelResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(ctx, "DrainHostModel", input).Get(ctx, nil); err != nil {
		return ModelResult{}, err
	}
	var status controlv1.HostModelStatus
	if err := workflow.ExecuteActivity(ctx, "StopHostModel", input).Get(ctx, &status); err != nil {
		return ModelResult{}, err
	}
	return ModelResult{ModelID: input.ModelID, State: status.State}, nil
}

func (a *Activities) EnsureHostModel(ctx context.Context, input ModelInput) (*controlv1.HostModelStatus, error) {
	request := connect.NewRequest(&controlv1.EnsureModelRequest{ModelId: input.ModelID})
	a.authorize(request.Header())
	response, err := a.hostAgent.EnsureModel(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
}

func (a *Activities) WaitKServeReady(ctx context.Context, input ModelInput) error {
	if input.KServeName == "" || input.ModelEndpoint == "" {
		return temporal.NewNonRetryableApplicationError("model has no KServe service endpoint", "invalid_catalog", nil)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, input.ModelEndpoint+"/health", nil)
	if err != nil {
		return temporal.NewNonRetryableApplicationError("invalid KServe model endpoint", "invalid_catalog", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("probe KServe model %s: %w", input.KServeName, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("KServe model %s returned readiness status %d", input.KServeName, response.StatusCode)
	}
	return nil
}

func (a *Activities) DrainHostModel(ctx context.Context, input ModelInput) (*controlv1.HostModelStatus, error) {
	request := connect.NewRequest(&controlv1.ModelLifecycleRequest{ModelId: input.ModelID})
	a.authorize(request.Header())
	response, err := a.hostAgent.DrainModel(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
}

func (a *Activities) StopHostModel(ctx context.Context, input ModelInput) (*controlv1.HostModelStatus, error) {
	request := connect.NewRequest(&controlv1.ModelLifecycleRequest{ModelId: input.ModelID})
	a.authorize(request.Header())
	response, err := a.hostAgent.StopModel(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
}

func (a *Activities) authorize(headers http.Header) {
	headers.Set("Authorization", "Bearer "+a.token)
}

func StartWorker(cfg config.ControlPlane, activities *Activities) (client.Client, func(), error) {
	options, err := clientOptions(cfg.Temporal)
	if err != nil {
		return nil, nil, err
	}
	temporalClient, err := client.Dial(options)
	if err != nil {
		return nil, nil, err
	}
	temporalWorker := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
	temporalWorker.RegisterWorkflow(LoadModelWorkflow)
	temporalWorker.RegisterWorkflow(DrainModelWorkflow)
	temporalWorker.RegisterWorkflow(UnloadModelWorkflow)
	temporalWorker.RegisterActivityWithOptions(activities.EnsureHostModel, activity.RegisterOptions{Name: "EnsureHostModel"})
	temporalWorker.RegisterActivityWithOptions(activities.WaitKServeReady, activity.RegisterOptions{Name: "WaitKServeReady"})
	temporalWorker.RegisterActivityWithOptions(activities.DrainHostModel, activity.RegisterOptions{Name: "DrainHostModel"})
	temporalWorker.RegisterActivityWithOptions(activities.StopHostModel, activity.RegisterOptions{Name: "StopHostModel"})
	if err := temporalWorker.Start(); err != nil {
		temporalClient.Close()
		return nil, nil, err
	}
	return temporalClient, func() {
		temporalWorker.Stop()
		temporalClient.Close()
	}, nil
}

func clientOptions(cfg config.Temporal) (client.Options, error) {
	options := client.Options{HostPort: cfg.Address, Namespace: cfg.Namespace}
	if cfg.TLSCACert == "" && cfg.TLSCert == "" && cfg.TLSKey == "" {
		return options, nil
	}
	if cfg.TLSCACert == "" || cfg.TLSCert == "" || cfg.TLSKey == "" {
		return client.Options{}, fmt.Errorf("Temporal CA, certificate, and key must be configured together")
	}
	certificate, err := tls.X509KeyPair([]byte(cfg.TLSCert), []byte(cfg.TLSKey))
	if err != nil {
		return client.Options{}, fmt.Errorf("parse Temporal client certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(cfg.TLSCACert)) {
		return client.Options{}, fmt.Errorf("parse Temporal CA certificate")
	}
	serverName, _, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		serverName = cfg.Address
	}
	options.ConnectionOptions.TLS = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}
	return options, nil
}
