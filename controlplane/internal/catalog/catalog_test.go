package catalog

import "testing"

func TestLoadsModelMetadataFromKServeConfig(t *testing.T) {
	catalog, err := Load("../../../models/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	model, ok := catalog.Get("mlx-community/SmolLM2-135M-Instruct")
	if !ok {
		t.Fatal("model not found")
	}
	if model.Model.TokenizerId != "HuggingFaceTB/SmolLM2-135M-Instruct" {
		t.Fatalf("unexpected tokenizer: %s", model.Model.TokenizerId)
	}
	if model.Model.ContextLimit != 4096 {
		t.Fatalf("unexpected context limit: %d", model.Model.ContextLimit)
	}
}
