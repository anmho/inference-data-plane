CONFIG_FILE ?= ./config/local.yaml
GKE_KUBE_CONTEXT ?= gke_anmho-infra-prod_us-central1-c_inference
export CONFIG_FILE

.PHONY: fmt lint test run model-start model-stop stack-start start stop compose-up compose-down budget bench e2e stream minikube-deploy cloudrun-build-push cloudrun-deploy cloudrun-crema-deploy cloudrun-gpu-deploy cloudrun-gpu-disable cloudrun-memorystore-up cloudrun-memorystore-down cloudrun-bench-once cloudrun-cleanup cloudrun-verify-idle gke-auth gke-build-push gke-deploy gke-forward gke-bench gke-vllm-deploy gke-vllm-bench gke-smoke-once gke-shutdown

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

run:
	cargo run -p inference-frontend

model-start:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/model-start.sh

model-stop:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/model-stop.sh

stack-start:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/stack-start.sh

start:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/start.sh

stop:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/model-stop.sh
	docker compose down -v

compose-up:
	CONFIG_FILE=$(CONFIG_FILE) docker compose up --build

compose-down:
	docker compose down -v

budget:
	cargo run -p token-budget-engine --bin budget -- crates/token-budget-engine/fixtures/long_prompt.txt --model local-vllm --max-tokens 256

bench:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/load.sh

e2e:
	CONFIG_FILE=$(CONFIG_FILE) ./scripts/e2e.sh

stream:
	. ./scripts/load-config.sh && BASE_URL=$${BASE_URL:-http://localhost:8080} MODEL=$${MODEL_ID} MAX_TOKENS=$${MAX_TOKENS:-$${STREAM_MAX_TOKENS:-96}} cargo run -q -p inference-frontend --bin connect_smoke

minikube-deploy:
	./scripts/minikube-deploy.sh

cloudrun-build-push:
	./scripts/cloudrun-build-push.sh

cloudrun-deploy:
	./scripts/cloudrun-deploy.sh

cloudrun-crema-deploy:
	./scripts/crema-deploy.sh

cloudrun-gpu-deploy:
	./scripts/cloudrun-gpu-deploy.sh

cloudrun-gpu-disable:
	./scripts/cloudrun-gpu-disable.sh

cloudrun-memorystore-up:
	./scripts/cloudrun-memorystore-up.sh

cloudrun-memorystore-down:
	./scripts/cloudrun-memorystore-down.sh

cloudrun-bench-once:
	./scripts/cloudrun-bench-once.sh

cloudrun-cleanup:
	./scripts/cloudrun-cleanup.sh

cloudrun-verify-idle:
	./scripts/cloudrun-verify-idle.sh

gke-auth:
	./scripts/gke-auth.sh

gke-build-push:
	./scripts/gke-build-push.sh

gke-deploy:
	KUBE_CONTEXT=$(GKE_KUBE_CONTEXT) ./scripts/gke-deploy.sh

gke-forward:
	KUBE_CONTEXT=$(GKE_KUBE_CONTEXT) ./scripts/gke-forward.sh

gke-bench:
	BASE_URL=$${BASE_URL:-http://localhost:8080} CONFIG_FILE=./config/synthetic.yaml ./scripts/load.sh

gke-vllm-deploy:
	KUBE_CONTEXT=$(GKE_KUBE_CONTEXT) ./scripts/gke-vllm-deploy.sh

gke-vllm-bench:
	KUBE_CONTEXT=$(GKE_KUBE_CONTEXT) ./scripts/gke-vllm-bench.sh

gke-smoke-once:
	KUBE_CONTEXT=$(GKE_KUBE_CONTEXT) ./scripts/gke-smoke-once.sh

gke-shutdown:
	./scripts/gke-shutdown.sh
