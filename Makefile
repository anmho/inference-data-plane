CONFIG_FILE ?= ./config/local.yaml
export CONFIG_FILE

.PHONY: fmt lint test start stop stream bench compose-up compose-down minikube-start kserve-install minikube-deploy model-start model-stop valkey-test

fmt:
	cargo fmt --all
	cd controlplane && gofmt -w .

lint:
	cargo clippy --workspace --all-targets -- -D warnings
	cd controlplane && go vet ./...

test:
	cargo test --workspace
	cd controlplane && go test ./...

start:
	./scripts/start.sh

stop:
	./scripts/host-agent-stop.sh

stream:
	. ./scripts/load-config.sh && BASE_URL=$${BASE_URL:-http://localhost:8080} MODEL=$${MODEL_ID} MAX_TOKENS=$${MAX_TOKENS:-$${STREAM_MAX_TOKENS:-96}} cargo run -q -p inference-frontend --bin connect_smoke

bench:
	RUN_BENCH=true ./scripts/start.sh

compose-up:
	CONFIG_FILE=$(CONFIG_FILE) docker compose up --build

compose-down:
	docker compose down -v

minikube-start:
	./scripts/minikube-start.sh

kserve-install:
	./scripts/kserve-install.sh

minikube-deploy:
	./scripts/minikube-deploy.sh

model-start:
	./scripts/model-start.sh

model-stop:
	./scripts/model-stop.sh

valkey-test:
	./scripts/valkey-test.sh
