# =============================================================================
# Module Inputs — Confluent Cloud Basic cluster
# =============================================================================

variable "display_name" {
  description = "Cluster display name shown in the Confluent Cloud Console. Lowercase, hyphens, 1-50 chars."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,49}$", var.display_name))
    error_message = "display_name must be lowercase alphanumeric with hyphens, 1-50 chars, starting with a letter."
  }
}

variable "environment_id" {
  description = "Confluent Cloud environment ID (e.g., env-12345). Must already exist; this module does not create environments."
  type        = string

  validation {
    condition     = can(regex("^env-[a-z0-9]+$", var.environment_id))
    error_message = "environment_id must match the Confluent Cloud env-* pattern (e.g., env-9y7opm)."
  }
}

variable "cloud" {
  description = "Cloud provider for the cluster. Basic supports AWS, GCP, and Azure."
  type        = string

  validation {
    condition     = contains(["AWS", "GCP", "AZURE"], var.cloud)
    error_message = "cloud must be one of: AWS, GCP, AZURE."
  }
}

variable "region" {
  description = "Cloud-provider region (e.g., us-east1 for GCP, us-east-1 for AWS, eastus for Azure). Confirm region availability via `confluent kafka region list` — Basic does not support every region."
  type        = string

  validation {
    condition     = length(var.region) > 0
    error_message = "region must be a non-empty string."
  }
}

variable "owner" {
  description = "Owning team email or distribution list. Recorded for governance; not enforced by Confluent Cloud at the cluster level."
  type        = string
  default     = ""
}
