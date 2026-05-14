# =============================================================================
# FSI Kafka Platform — Confluent Cloud Basic Cluster Module
# =============================================================================
# Provisions a single Basic-tier Confluent Cloud Kafka cluster inside an
# existing environment. Outputs the IDs that scenario/cc-* starter kits
# need in their clusters.auto.tfvars.
#
# Per Canon defaults + FSI overlay, Basic is suitable ONLY for:
#   - smoke tests and throwaway demos
#   - sandbox / dev environments where DR is not required
# Basic CANNOT satisfy the FSI production canon — see "Limitations" below.
# For production FSI workloads use a sibling module/cc-cluster-standard or
# module/cc-cluster-enterprise (out of scope for this initial cut).
#
# Usage:
#   module "franz_smoke" {
#     source         = "../../modules/cc-cluster-basic"
#     display_name   = "franz-smoke-01"
#     environment_id = "env-9y7opm"
#     cloud          = "GCP"
#     region         = "us-east1"
#     owner          = "platform-team@fsi.org"
#   }
#
# Limitations (Basic tier — confirm via Confluent docs before quoting):
#   - SINGLE_ZONE availability only (no multi-AZ)
#   - No Cluster Linking destination (Basic CAN be a CL source as of 2026-05)
#   - No mTLS authentication (API keys / OAuth only)
#   - No private networking (public endpoint only)
#   - No BYOK / customer-managed encryption
#   - No Stream Governance Advanced features
# =============================================================================

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

# ---------------------------------------------------------------------------
# Environment resolution
# ---------------------------------------------------------------------------
# Read-only reference — the cluster lives inside an env owned by the
# platform team. Creating environments is out of scope for this module.

data "confluent_environment" "target" {
  id = var.environment_id
}

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

resource "confluent_kafka_cluster" "this" {
  display_name = var.display_name
  availability = "SINGLE_ZONE" # Basic is single-zone only — provider rejects MULTI_ZONE
  cloud        = var.cloud
  region       = var.region

  # Tier selector. Confluent provider requires exactly one of:
  #   basic {} | standard {} | enterprise {} | freight {} | dedicated { cku = N }
  basic {}

  environment {
    id = data.confluent_environment.target.id
  }

  lifecycle {
    # Cheap insurance against accidental teardown via re-applies. Operators
    # who genuinely want to delete a cluster must `terraform state rm` first
    # or remove this lifecycle block in a separate PR.
    prevent_destroy = true
  }
}
