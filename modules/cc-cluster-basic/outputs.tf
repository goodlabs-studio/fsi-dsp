# =============================================================================
# Module Outputs — exactly the surface scenario/cc-* expects in
# clusters.auto.tfvars so the post-cluster topic + SR + RBAC + DR layer
# can chain off this module's output.
# =============================================================================

output "kafka_cluster_id" {
  description = "Cluster ID (lkc-xxxxx). Feed into scenario/cc-*.clusters.auto.tfvars as kafka_cluster_id."
  value       = confluent_kafka_cluster.this.id
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint URL. Feed into scenario/cc-*.clusters.auto.tfvars as kafka_rest_endpoint."
  value       = confluent_kafka_cluster.this.rest_endpoint
}

output "kafka_cluster_crn" {
  description = "Cluster CRN, used for RBAC role bindings. Feed into scenario/cc-*.clusters.auto.tfvars as kafka_cluster_crn."
  value       = confluent_kafka_cluster.this.rbac_crn
}

output "bootstrap_endpoint" {
  description = "Bootstrap endpoint URL (host:port) for client producer/consumer config (the BOOTSTRAP_SERVERS env var). Note: this is the public endpoint; Basic clusters do not support private networking."
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "environment_id" {
  description = "Environment ID the cluster lives in (echoed from input for downstream module convenience)."
  value       = data.confluent_environment.target.id
}

output "display_name" {
  description = "Cluster display name (echoed from input)."
  value       = confluent_kafka_cluster.this.display_name
}

output "tier" {
  description = "Cluster tier — always 'basic' for this module. Downstream consumers can branch on tier for tier-specific guards (e.g., refuse to attach Cluster Linking destination)."
  value       = "basic"
}
