package connectapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	controlv1 "github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1"
	"github.com/anmho/inference-data-plane/controlplane/internal/catalog"
	"github.com/anmho/inference-data-plane/controlplane/internal/config"
	"github.com/anmho/inference-data-plane/controlplane/internal/lifecycle"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type ModelControl struct {
	catalog  *catalog.Catalog
	temporal client.Client
	config   config.ControlPlane
}

func NewModelControl(catalog *catalog.Catalog, temporalClient client.Client, cfg config.ControlPlane) *ModelControl {
	return &ModelControl{catalog: catalog, temporal: temporalClient, config: cfg}
}

func (h *ModelControl) ListModels(context.Context, *connect.Request[controlv1.ListModelsRequest]) (*connect.Response[controlv1.ListModelsResponse], error) {
	return connect.NewResponse(&controlv1.ListModelsResponse{Models: h.catalog.List()}), nil
}

func (h *ModelControl) GetModel(_ context.Context, request *connect.Request[controlv1.GetModelRequest]) (*connect.Response[controlv1.Model], error) {
	entry, ok := h.catalog.Get(request.Msg.ModelId)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("model %q is not registered", request.Msg.ModelId))
	}
	return connect.NewResponse(entry.Model), nil
}

func (h *ModelControl) ResolveModel(_ context.Context, request *connect.Request[controlv1.ResolveModelRequest]) (*connect.Response[controlv1.ResolvedModel], error) {
	entry, ok := h.catalog.Get(request.Msg.ModelId)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("model %q is not registered", request.Msg.ModelId))
	}
	return connect.NewResponse(&controlv1.ResolvedModel{
		Model:    entry.Model,
		Endpoint: entry.Endpoint,
		Status:   "registered",
	}), nil
}

func (h *ModelControl) LoadModel(ctx context.Context, request *connect.Request[controlv1.ModelLifecycleRequest]) (*connect.Response[controlv1.ModelOperation], error) {
	return h.start(ctx, request.Msg.ModelId, "load", lifecycle.LoadModelWorkflow)
}

func (h *ModelControl) DrainModel(ctx context.Context, request *connect.Request[controlv1.ModelLifecycleRequest]) (*connect.Response[controlv1.ModelOperation], error) {
	return h.start(ctx, request.Msg.ModelId, "drain", lifecycle.DrainModelWorkflow)
}

func (h *ModelControl) UnloadModel(ctx context.Context, request *connect.Request[controlv1.ModelLifecycleRequest]) (*connect.Response[controlv1.ModelOperation], error) {
	return h.start(ctx, request.Msg.ModelId, "unload", lifecycle.UnloadModelWorkflow)
}

func (h *ModelControl) start(ctx context.Context, modelID, operation string, workflowFn any) (*connect.Response[controlv1.ModelOperation], error) {
	entry, ok := h.catalog.Get(modelID)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("model %q is not registered", modelID))
	}
	workflowID := fmt.Sprintf("model-%s-%x", operation, sha256.Sum256([]byte(modelID)))[:48]
	_, err := h.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             h.config.Temporal.TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, workflowFn, lifecycle.ModelInput{
		ModelID:       modelID,
		ModelEndpoint: entry.Endpoint,
		KServeName:    entry.KServeService,
		Namespace:     h.config.Namespace,
	})
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &alreadyStarted) {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("start lifecycle workflow: %w", err))
		}
	}
	return connect.NewResponse(&controlv1.ModelOperation{
		WorkflowId: workflowID,
		ModelId:    modelID,
		Operation:  operation,
	}), nil
}
