terraform {
  required_version = ">= 1.6.0"

  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

variable "kube_context" {
  description = "Local Kubernetes context to deploy into."
  type        = string
  default     = "minikube"
}

variable "manifest_dir" {
  description = "Kustomize directory for the local data-plane deployment."
  type        = string
  default     = "../../k8s/base"
}

locals {
  manifest_files = fileset(var.manifest_dir, "*.yaml")
  manifest_hashes = {
    for file in local.manifest_files :
    file => filesha256("${var.manifest_dir}/${file}")
  }
}

resource "null_resource" "local_inference_data_plane" {
  triggers = {
    kube_context    = var.kube_context
    manifest_hashes = jsonencode(local.manifest_hashes)
  }

  provisioner "local-exec" {
    command = "kubectl --context ${var.kube_context} apply -k ${var.manifest_dir}"
  }
}

output "namespace" {
  value = "inference-data-plane"
}

output "port_forward" {
  value = "kubectx ${var.kube_context} && kubens inference-data-plane && kubectl port-forward svc/inference-frontend 8080:8080"
}

output "e2e" {
  value = "BASE_URL=http://localhost:8080 ./scripts/e2e.sh"
}
