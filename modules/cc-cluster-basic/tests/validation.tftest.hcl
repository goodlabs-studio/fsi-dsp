# =============================================================================
# Validation Tests — input regex and tier-by-cloud constraints
# =============================================================================
# Uses mock provider so the test suite runs without live Confluent Cloud
# credentials. These tests cover the variable validation surface; an
# integration test against a real CC sandbox is out of scope here.

mock_provider "confluent" {}

variables {
  display_name   = "franz-smoke-01"
  environment_id = "env-9y7opm"
  cloud          = "GCP"
  region         = "us-east1"
  owner          = "platform-team@fsi.org"
}

# ---------------------------------------------------------------------------
# Happy path — valid input → cluster gets the expected attributes
# ---------------------------------------------------------------------------

run "happy_path_gcp_basic" {
  command = plan

  assert {
    condition     = confluent_kafka_cluster.this.display_name == "franz-smoke-01"
    error_message = "display_name should round-trip from variable."
  }

  assert {
    condition     = confluent_kafka_cluster.this.availability == "SINGLE_ZONE"
    error_message = "Basic tier must hardcode SINGLE_ZONE — provider rejects MULTI_ZONE for basic{}."
  }

  assert {
    condition     = confluent_kafka_cluster.this.cloud == "GCP"
    error_message = "cloud must round-trip from variable."
  }

  assert {
    condition     = output.tier == "basic"
    error_message = "tier output must always be 'basic' for this module — downstream consumers branch on it."
  }
}

# ---------------------------------------------------------------------------
# Validation: display_name regex
# ---------------------------------------------------------------------------

run "reject_display_name_uppercase" {
  command = plan
  variables { display_name = "FranzSmoke" }
  expect_failures = [var.display_name]
}

run "reject_display_name_starts_with_digit" {
  command = plan
  variables { display_name = "1cluster" }
  expect_failures = [var.display_name]
}

run "reject_display_name_too_long" {
  command = plan
  variables { display_name = "this-name-is-far-too-long-to-be-a-valid-cluster-name-it-keeps-going" }
  expect_failures = [var.display_name]
}

# ---------------------------------------------------------------------------
# Validation: environment_id pattern
# ---------------------------------------------------------------------------

run "reject_environment_id_wrong_prefix" {
  command = plan
  variables { environment_id = "lkc-12345" } # cluster ID, not env ID
  expect_failures = [var.environment_id]
}

run "reject_environment_id_empty" {
  command = plan
  variables { environment_id = "" }
  expect_failures = [var.environment_id]
}

# ---------------------------------------------------------------------------
# Validation: cloud enum
# ---------------------------------------------------------------------------

run "reject_cloud_lowercase" {
  command = plan
  variables { cloud = "gcp" }
  expect_failures = [var.cloud]
}

run "reject_cloud_unknown" {
  command = plan
  variables { cloud = "ORACLE" }
  expect_failures = [var.cloud]
}

# ---------------------------------------------------------------------------
# Per-cloud sanity — region passes through unchanged for each supported cloud
# ---------------------------------------------------------------------------

run "aws_us_east_1" {
  command = plan
  variables {
    cloud  = "AWS"
    region = "us-east-1"
  }
  assert {
    condition     = confluent_kafka_cluster.this.region == "us-east-1"
    error_message = "AWS region should round-trip."
  }
}

run "azure_eastus" {
  command = plan
  variables {
    cloud  = "AZURE"
    region = "eastus"
  }
  assert {
    condition     = confluent_kafka_cluster.this.region == "eastus"
    error_message = "Azure region should round-trip."
  }
}
