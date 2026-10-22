# FSI Data Streaming Platform

A universal, automation-first platform for standing up governed Kafka, Flink, and Schema Registry infrastructure across any deployment model. Packages FSI-specific C4E assets into scenario-based starter kits that any financial institution can adopt in hours, not months.

## Deployment Models

| Scenario | Path | Infrastructure |
|----------|------|----------------|
| **Confluent Cloud — AWS** | `scenarios/cc-aws/` | Terraform |
| **Confluent Cloud — Azure** | `scenarios/cc-azure/` | Terraform |
| **Confluent Cloud — GCP** | `scenarios/cc-gcp/` | Terraform |
| **CFK on OpenShift** | `scenarios/cfk-openshift/` | Helm / CFK Operator |
| **CP on RHEL** | `scenarios/cp-rhel/` | Ansible / systemd |
| **Private Cloud** | `scenarios/private-cloud/` | Terraform |
| **CP on RHEL on LinuxONE** | `scenarios/cp-rhel-linuxone/` | Ansible / systemd |
| **CFK on OpenShift on LinuxONE** | `scenarios/cfk-openshift-linuxone/` | Helm / CFK Operator |

All eight scenarios enforce identical governance: topic naming, schema compatibility, RBAC patterns, and SLA-tier defaults.

### Lakehouse integration (ADR-011)

All scenarios support Databricks and Snowflake ingest:

| Path | Mechanism | CC | CP / CFK / LinuxONE |
|---|---|---|---|
| **DB-A** | Confluent Tableflow → Databricks Unity Catalog | ✅ via `modules/tableflow/` | ❌ Tableflow is CC-only — fall back to DB-C |
| **DB-C** | Databricks Delta Lake Sink Connector | ✅ via `modules/lakehouse_sink/` (managed) | ✅ via `reference/connect-configs/` + `ansible/roles/cp_databricks_sink` (self-managed) |
| **SF-A** | Snowflake Snowpipe Streaming Connector | ✅ via `modules/lakehouse_sink/` (managed) | ✅ via `reference/connect-configs/` + `ansible/roles/cp_snowflake_sink` (self-managed) |
| **SF-B** | Tableflow → Iceberg + Open Data Catalog → Snowflake | ✅ via `modules/tableflow/` | ❌ Tableflow is CC-only — fall back to SF-A |

See [ADR-011](docs/adr/011-lakehouse-integration-patterns.md),
[Databricks Integration Guide](docs/databricks-integration-guide.md),
[Snowflake Integration Guide](docs/snowflake-integration-guide.md), and
[Tableflow Guide](docs/tableflow-guide.md).

### Database integration (ADR-012)

All scenarios support MongoDB, Redis, CockroachDB, and PostgreSQL connectors
— both CDC source and sink directions where applicable:

| Path | Source (Kafka ← DB) | Sink (Kafka → DB) | CC managed | Self-managed |
|---|---|---|---|---|
| **MongoDB** | Change streams via `MongoSourceConnector` | `MongoSinkConnector` | ✅ via `modules/db_connector/` | ✅ via `reference/connect-configs/mongodb-*-example.json` + `ansible/roles/cp_mongodb/` |
| **Redis** | ❌ (cache, not source of truth) | `RedisSinkConnector` | ✅ via `modules/db_connector/` | ✅ via `reference/connect-configs/redis-sink-example.json` + `ansible/roles/cp_redis/` |
| **CockroachDB** | ✅ Native `CREATE CHANGEFEED` SQL (no Kafka Connect — DBA-driven, see [`reference/cockroachdb/changefeed-examples.sql`](reference/cockroachdb/changefeed-examples.sql)) | JDBC sink with Postgres driver on port 26257 | ✅ via `modules/db_connector/` (sink only) | ✅ via `reference/connect-configs/cockroachdb-jdbc-sink-example.json` + `ansible/roles/cp_cockroachdb/` |
| **PostgreSQL** | Debezium `PostgresConnector` (logical decoding) | `JdbcSinkConnector` | ✅ via `modules/db_connector/` | ✅ via `reference/connect-configs/postgres-*-example.json` + `ansible/roles/cp_postgres/` |

See [ADR-012](docs/adr/012-database-connector-patterns.md),
[MongoDB Integration Guide](docs/mongodb-integration-guide.md),
[Redis Integration Guide](docs/redis-integration-guide.md),
[CockroachDB Integration Guide](docs/cockroachdb-integration-guide.md), and
[PostgreSQL Integration Guide](docs/postgres-integration-guide.md).

### Mainframe integration: IBM MQ on z/OS (`scenarios/mq-quickstart/`)

Kafka Connect on Z: the worker runs as a z/OS started task, dispatched on zIIP,
next to the queue manager, so the IBM MQ source and sink connectors use
bindings mode (cross-memory, no channel). An Ansible playbook on
`ibm.ibm_zos_core` with one inputs file and a preflight. RACF and PROCLIB jobs
are generated and staged for the security team and systems programmer, never
submitted. The LinuxONE / client-mode path is [ADR-009](docs/adr/009-linuxone-kafka-offload.md).

## Accelerators

Where `scenarios/` are starter kits, `accelerators/` are opinionated, end-to-end, production-grade deployments.

### Confluent on LinuxONE (`accelerators/confluent-on-linuxone/`)

FSI-hardened Confluent Platform on IBM LinuxONE (s390x) via the CFK operator on OpenShift. Forks IBM / Matt Mondics's public reference runbook (pulled by pinned SHA — fetch-by-SHA, not vendored) as a clean base, then layers five Kustomize Components on top:

| Layer | Control |
|-------|---------|
| `01-rbac` | MDS RBAC — platform-admin, topic-admin, producer/consumer-only, auditor-readonly, schema-admin |
| `02-tls` | mTLS between all components, FIPS cipher suites, cert-manager rotation |
| `03-schema-governance` | FULL_TRANSITIVE compatibility, subject-naming enforcement, hard-delete controls |
| `04-audit` | Broker audit log → 7-year retention topic → Splunk / Dynatrace SIEM sinks |
| `05-flink` | Apache Flink via Confluent Manager for Apache Flink (CMF) — `FlinkApplication` CRs, self-contained mTLS + RBAC, FSI example jobs |

Composed by `overlays/{dev,prod}`; `flox activate` pins the toolchain. Flink's prerequisite operators (FKO + CMF) install via the `flink_operators` Ansible role. See `accelerators/confluent-on-linuxone/README.md`, `DESIGN.md`, and `KNOWN-GAPS.md`.

## Quick Start

```bash
# 1. Pick your deployment model
cd scenarios/cc-aws/          # or cc-azure, cc-gcp, cfk-openshift, cp-rhel, private-cloud

# 2. Configure environment
cp .env.example .env          # fill in cluster details
# For Terraform scenarios: cp terraform.tfvars.example terraform.tfvars
# For CFK: edit values/*.yaml
# For CP-RHEL: edit inventory/hosts.yml.example → hosts.yml

# 3. Deploy
# CC scenarios:    terraform init && terraform apply
# CFK on OCP:     helm install confluent-operator ... -f values/kafka.yaml
# CP on RHEL:     ansible-playbook -i inventory/hosts.yml playbooks/deploy-cp.yml

# 4. Ansible-driven governance (CP/CFK deployments)
cd ansible/
ansible-playbook site.yml                    # full stack: cluster + topics + schemas + RBAC + connectors + observability
ansible-playbook site.yml --tags topics      # just topic governance
ansible-playbook site.yml --tags rbac        # just RBAC bindings
ansible-playbook site.yml --check            # dry-run / audit mode
```

## What's Included

### Shared Governance (`modules/`)

- **`modules/topic/`** — Single Terraform module: topic + Avro schema + RBAC + metadata tags + DR mirror
- **`modules/flink/`** — CC Flink compute pool with SQL statement management
- SLA-tier-based defaults (critical / standard / best-effort) drive partitions, retention, compatibility, and DR thresholds automatically

### DR Automation (`scripts/` + `ansible/playbooks/`)

**Shell CLI** — unified interface with pluggable backends:

```bash
./scripts/fsi-dr.sh failover --backend cl    # Cluster Linking (CC)
./scripts/fsi-dr.sh failover --backend mm2   # MirrorMaker 2 (CFK/CP)
./scripts/fsi-dr.sh failover --backend mrc   # Multi-Region Cluster RPO=0 (CP)
./scripts/fsi-dr.sh status   --backend cl    # Mirror lag + health
```

**Ansible playbooks** — DR operations with check-mode audit and compliance output:

```bash
ansible-playbook playbooks/dr-failover-mm2.yml           # MM2 failover (6-step sequence)
ansible-playbook playbooks/dr-failover-mrc.yml           # MRC observer promotion (RPO=0)
ansible-playbook playbooks/dr-failover-mm2.yml --check   # audit-ready dry run
ansible-playbook playbooks/dr-drill.yml                  # full DR drill: failover → validate → failback → validate → compliance report
```

Features: dry-run mode, state validation, rollback, Consul-based atomic failover, SLA-tier lag thresholds, quarterly DR drill automation with OCC/FDIC compliance reports. 103 shell tests + 662 Ansible tests.

### Observability (`observability/`)

Pre-built dashboard templates for six providers — import and go:

| Provider | Dashboards |
|----------|-----------|
| Dynatrace | Cluster health, app view, Connect, DR readiness |
| Datadog | Cluster health, app view, Connect, DR readiness |
| Splunk | Cluster health, app view, Connect, DR readiness |
| Grafana/Prometheus | Cluster health, app view, Connect, DR readiness |
| New Relic | Cluster health, app view, Connect, DR readiness |
| IBM Instana | Cluster health, app view, Connect, DR readiness |

Auto-discovery rules, SLA-tier alert thresholds, and JMX exporter configs included.

### Flink Streaming (`modules/flink/`, `scenarios/*/flink/`)

Reference SQL templates across all deployment models:

- **CC Flink** — Terraform-managed compute pools with SQL statements
- **CFK Flink** — Flink Kubernetes Operator with FlinkDeployment CRDs
- **Standalone Flink** — Ansible role with systemd (JobManager + TaskManager)

All include: tumbling window aggregation, stream-table join enrichment, filter-and-route fan-out, Avro/SR integration.

### Reference Implementations (`reference/`)

| Language | Producer | Consumer | DLQ |
|----------|----------|----------|-----|
| Java 17 | Idempotent, Avro, JMX | Manual commit, graceful shutdown | 3-retry backoff, error categorization |
| .NET | Confluent.Kafka client | Schema Registry integration | 3-retry backoff, error categorization |
| Python | confluent-kafka-python | Avro deserialization | 3-retry backoff, error categorization |

Plus: Kafka Connect JDBC configs (East/West), Docker Compose local dev (Kafka + SR + Connect + Flink), integration test roundtrip.

### Security & Compliance

- **RBAC**: CC role bindings (Terraform), CFK ACLs + MDS (Helm), CP MDS (Ansible)
- **Encryption**: mTLS across all deployment models, cert-manager for CFK
- **Secrets**: Vault, Azure Key Vault, AWS Secrets Manager, Google Secret Manager
- **FIPS 140-2**: Automated validation for CP on RHEL and CFK on FIPS-enabled OpenShift (`scripts/validate-fips.sh`)
- **Compliance**: Configurable retention up to 7 years (OFAC/AML), schema evolution enforcement in CI

### Ansible Automation (`ansible/`)

Eleven roles providing full lifecycle management for Confluent Platform and CFK deployments:

| Role | Purpose |
|------|---------|
| `cp_topic` | Topic CRUD via Admin REST v3 with SLA-tier governance |
| `cp_schema` | Avro schema registration with two-pass compatibility safety |
| `cp_rbac` | MDS RBAC binding lifecycle with LIST/DIFF/ADD/REMOVE reconciliation |
| `cp_connect` | Connector lifecycle via idempotent PUT REST API |
| `cp_observability` | JMX exporter, Prometheus file_sd, Grafana dashboards, SLA-tier alerts |
| `cp_dr_mm2` | MM2 failover/failback with Consul flip and state validation |
| `cp_dr_mrc` | MRC observer promotion for RPO=0 scenarios |
| `cfk_operator` | CFK Helm deployment with CR readiness gates |
| `cfk_topic` | KafkaTopic CRD generation from CPTopic YAML with governance parity |
| `cp_mtls` | mTLS certificate provisioning (CA, broker/client keystores, truststores) |
| `flink_operators` | Flink Kubernetes Operator + Confluent Manager for Apache Flink (CMF) Helm install with readiness gates |

All roles support `--check` mode for audit-ready dry runs. Orchestrated by `site.yml` with tag-isolated selective execution.

### CI/CD (`ci/`, `.github/`)

- **C4E Pre-check** — Validates topic naming, SLA tiers, schema compatibility, retention, and RBAC across Terraform, CFK YAML, and CPTopic formats
- **Schema Validation** — Avro syntax + compatibility checks on every PR
- **Terraform Plan/Apply** — Plan on PR, apply on merge to main
- **Ansible CI** — ansible-lint, yamllint, and molecule tests on every PR touching `ansible/`
- **Override Detection** — Flags governance overrides for C4E review

## Project Structure

```
accelerators/
  confluent-on-linuxone/     # FSI-hardened CP on LinuxONE — CFK base + 5 Kustomize layers
                             #   (RBAC, TLS, schema governance, audit, Flink)
scenarios/
  cc-aws/                    # Confluent Cloud on AWS (Terraform)
  cc-azure/                  # Confluent Cloud on Azure (Terraform)
  cc-gcp/                    # Confluent Cloud on GCP (Terraform)
  cfk-openshift/             # CFK on OpenShift (Helm values, CRDs, Flink operator)
  cp-rhel/                   # Confluent Platform on RHEL (Ansible, systemd)
  private-cloud/             # Confluent Private Cloud (Terraform)
  cp-rhel-linuxone/          # CP on RHEL on LinuxONE s390x (Ansible)
  cfk-openshift-linuxone/    # CFK on OpenShift on LinuxONE s390x (Helm)
  mq-quickstart/             # Kafka Connect on Z + IBM MQ bindings mode (Ansible, ibm_zos_core)
modules/
  topic/                     # Shared governance module (topic + schema + RBAC + DR)
  flink/                     # CC Flink compute pool module
ansible/
  roles/
    cp_topic/                # Topic lifecycle (Admin REST v3)
    cp_schema/               # Schema registration (SR REST API)
    cp_rbac/                 # MDS RBAC binding reconciliation
    cp_connect/              # Connector lifecycle (PUT REST API)
    cp_observability/        # JMX, Prometheus, Grafana, alerts
    cp_dr_mm2/               # MM2 failover/failback
    cp_dr_mrc/               # MRC observer promotion (RPO=0)
    cfk_operator/            # CFK Helm + CR readiness gates
    cfk_topic/               # KafkaTopic CRD from CPTopic YAML
    cp_mtls/                 # mTLS cert provisioning (CA, keystores, truststores)
    flink_operators/         # FKO + CMF Helm install (readiness-gated)
  playbooks/                 # DR, governance, CFK deployment playbooks
  filter_plugins/            # fsi_governance Jinja2 filters
  inventories/               # dev, staging, prod, dr environments
  vars/                      # sla_tiers.yml, naming_rules.yml
  site.yml                   # End-to-end orchestration pipeline
scripts/
  fsi-dr.sh                  # Unified DR CLI (CL, MM2, MRC backends)
  validate-fips.sh           # FIPS 140-2 compliance validation
  validate-apply.sh          # Post-apply Terraform validation
observability/
  dynatrace/                 # Dashboard JSON templates
  datadog/                   # Dashboard JSON templates
  splunk/                    # Dashboard JSON templates
  grafana/                   # Dashboard JSON + Prometheus rules
  newrelic/                  # Dashboard JSON templates
  instana/                   # Dashboard JSON templates
reference/
  java-producer/             # Java 17 reference producer
  java-consumer/             # Java 17 reference consumer
  python-producer/           # Python reference producer
  python-consumer/           # Python reference consumer
  dotnet-producer/           # .NET reference producer
  dotnet-consumer/           # .NET reference consumer
  flink-sql/                 # Flink SQL templates
  connect-configs/           # JDBC source/sink configs
  local-dev/                 # Docker Compose dev environment
  integration-test/          # Roundtrip verification
docs/
  adr/                       # Architecture Decision Records
  dr-runbook.md              # DR procedures (CL, MM2, MRC)
  schema-guide.md            # Schema governance guide
  onboarding.md              # Team onboarding flow
  compliance-guide.md        # FSI compliance reference
  rotation-runbook.md        # Credential rotation procedures
ci/
  scripts/                   # C4E pre-check, schema validation
tests/
  dr/                        # DR backend unit tests (103 tests)
  ansible/                   # Ansible role unit tests (662 tests)
```

## Documentation

- **[DR Runbook](docs/dr-runbook.md)** — Failover/failback procedures for all three backends (CL, MM2, MRC) plus Ansible playbook operations
- **[Schema Guide](docs/schema-guide.md)** — Naming, compatibility modes, evolution rules
- **[Onboarding](docs/onboarding.md)** — Self-service intake form and team onboarding
- **[Cloud Providers](docs/cloud-providers.md)** — AWS vs Azure vs GCP differences
- **[Compliance Guide](docs/compliance-guide.md)** — FSI regulatory requirements and DR drill compliance reporting
- **[Credential Rotation](docs/rotation-runbook.md)** — Zero-downtime rotation procedures
- **[LinuxONE mTLS Guide](docs/linuxone-mtls-guide.md)** -- Certificate provisioning on s390x
- **[LinuxONE Troubleshooting](docs/linuxone-troubleshooting.md)** -- s390x TLS debug guide
- **[LinuxONE FIPS](docs/linuxone-fips-guide.md)** -- s390x FIPS validation
- **[LinuxONE CEX/HSM](docs/linuxone-cex-guide.md)** -- Hardware security module guide
- **[ADRs](docs/adr/)** — Architecture Decision Records

## License

Proprietary — GoodLabs Studio
