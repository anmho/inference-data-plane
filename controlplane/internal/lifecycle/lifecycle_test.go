package lifecycle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	controlv1 "github.com/anmho/inference-data-plane/controlplane/gen/inference/control/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

type fakeActivities struct{}

func (fakeActivities) ensure(context.Context, ModelInput) (*controlv1.HostModelStatus, error) {
	return &controlv1.HostModelStatus{ModelId: "model", State: "ready", Endpoint: "http://model"}, nil
}

func TestWaitKServeReadyProbesServingEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			t.Fatalf("unexpected readiness path %q", request.URL.Path)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	activities := NewActivities("http://unused", "unused")
	if err := activities.WaitKServeReady(context.Background(), ModelInput{
		KServeName:    "model",
		ModelEndpoint: server.URL,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWaitKServeReadyRejectsUnavailableEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	activities := NewActivities("http://unused", "unused")
	if err := activities.WaitKServeReady(context.Background(), ModelInput{
		KServeName:    "model",
		ModelEndpoint: server.URL,
	}); err == nil {
		t.Fatal("expected unavailable model endpoint to fail readiness")
	}
}

func (fakeActivities) ready(context.Context, ModelInput) error { return nil }

func (fakeActivities) drain(context.Context, ModelInput) (*controlv1.HostModelStatus, error) {
	return &controlv1.HostModelStatus{ModelId: "model", State: "draining"}, nil
}

func (fakeActivities) stop(context.Context, ModelInput) (*controlv1.HostModelStatus, error) {
	return &controlv1.HostModelStatus{ModelId: "model", State: "stopped"}, nil
}

func TestLoadWorkflowWaitsForHostAndKServe(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	fake := fakeActivities{}
	environment.RegisterActivityWithOptions(fake.ensure, activity.RegisterOptions{Name: "EnsureHostModel"})
	environment.RegisterActivityWithOptions(fake.ready, activity.RegisterOptions{Name: "WaitKServeReady"})
	environment.ExecuteWorkflow(LoadModelWorkflow, ModelInput{ModelID: "model", KServeName: "model", Namespace: "test"})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result ModelResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "ready" || result.Endpoint != "http://model" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestUnloadWorkflowDrainsBeforeStopping(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	fake := fakeActivities{}
	environment.RegisterActivityWithOptions(fake.drain, activity.RegisterOptions{Name: "DrainHostModel"})
	environment.RegisterActivityWithOptions(fake.stop, activity.RegisterOptions{Name: "StopHostModel"})
	environment.ExecuteWorkflow(UnloadModelWorkflow, ModelInput{ModelID: "model"})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result ModelResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "stopped" {
		t.Fatalf("unexpected result: %#v", result)
	}
}
