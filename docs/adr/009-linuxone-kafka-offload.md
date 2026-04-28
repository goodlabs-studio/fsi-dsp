# ADR-009: LinuxONE as Preferred Compute for z/OS Kafka Offload

**Status:** Accepted
**Date:** 2026-04-28
**Author:** Jeremy Hogan

## Context

Financial institutions running mainframe workloads on IBM z/OS need to integrate
legacy transaction systems with modern event-driven architectures built on Apache Kafka.
Two patterns exist for bridging z/OS data into Kafka:

1. **Direct z/OS Connect**: Kafka clients run on z/OS itself, limited by zIIP engine
   capacity, JVM tuning constraints, and license cost per MSU.
2. **Offload to Linux**: Kafka producers/consumers run on a Linux partition adjacent
   to the z/OS LPAR, connected via HiperSockets or internal coupling facility links.

IBM LinuxONE (Emperor/Rockhopper III+) provides a native Linux environment on the
same physical frame as z/OS, offering sub-millisecond latency to z/OS data via
HiperSockets while running standard Linux workloads (Kafka clients, Flink, Schema
Registry) without mainframe license overhead.

The canonical bridge pattern for z/OS-to-Kafka integration is the IBM MQ Source
Connector: z/OS applications write to MQ queues (a well-understood mainframe pattern),
and the Kafka Connect MQ Source Connector on LinuxONE consumes from those queues and
produces to Kafka topics.

## Decision

We adopt IBM LinuxONE as the preferred compute platform for z/OS Kafka offload
workloads in FSI engagements. The bridge pattern is:

```
z/OS Application -> IBM MQ Queue -> MQ Source Connector (LinuxONE) -> Kafka Topic
```

Key specifics:

- **Compute**: IBM LinuxONE Emperor 4 or Rockhopper III+ running RHEL 8.x/9.x or
  Ubuntu 22.04 LTS
- **Network**: HiperSockets for z/OS-to-LinuxONE communication (sub-millisecond,
  no external network hop)
- **Bridge**: Confluent-certified IBM MQ Source Connector for Kafka Connect,
  running in distributed mode on LinuxONE
- **Serialization**: Avro with Schema Registry (per ADR-001); schemas registered
  at the connector level
- **Security**: FIPS 140-2 validated cryptographic modules on LinuxONE; mTLS between
  Kafka clients and brokers (per ADR-006)
- **DR**: LinuxONE instances in both primary and DR regions; Cluster Linking
  replicates topics cross-region (per ADR-005)

## Consequences

**Easier:**
- Mainframe teams continue writing to MQ (no application changes on z/OS)
- Sub-millisecond bridge latency suitable for `market_data` and `risk` SLA tiers
- LinuxONE inherits z/OS availability (99.999%) without mainframe software license cost
- FIPS compliance is native; no additional certification work for crypto modules
- Standard Linux tooling (Ansible, Terraform, Docker) works on LinuxONE

**Harder:**
- LinuxONE hardware procurement has long lead times (6-12 months)
- Teams unfamiliar with mainframe infrastructure need LinuxONE-specific runbooks
- MQ Source Connector requires MQ queue manager configuration on z/OS side
  (coordination with mainframe team)
- HiperSockets configuration requires IBM z/VM or PR/SM setup expertise
- Not all Confluent components are certified for s390x architecture; verify
  connector and client versions before deployment

**Mitigations:**
- Provide pre-built Ansible roles for LinuxONE Kafka Connect deployment
  (future: `role/linuxone_connect`)
- Document MQ queue manager setup in partnership with IBM mainframe practice
- Maintain a LinuxONE compatibility matrix for Confluent component versions
