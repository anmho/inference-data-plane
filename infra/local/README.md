# Local Terraform

This Terraform root is intentionally local-only. It uses Terraform local state and applies the minikube data-plane manifests through `kubectl apply -k`.

It does not provision GCP, GKE, GPU node pools, or persistent storage. Those belong in a later production environment root.

```bash
terraform -chdir=infra/local init
terraform -chdir=infra/local apply
```

The deployed Valkey instance is a single ephemeral in-cluster broker. It has no PVC and no append-only file because the local demo does not need durable stream storage.
