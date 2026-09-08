# CFK + CMF Flink + Connect on OpenShift on LinuxONE — Demo Cluster

A basic, runnable Confluent Platform demo on IBM z16 (Telum) under OpenShift,
with a MongoDB source→Flink→sink round trip. Sized from
[`z-stream-sizer`](https://github.com/goodlabs-studio/z-sizer) against a
dedicated-IFL z16 LPAR.

> **Demo-grade. Not production.** No mTLS, no RBAC, no Vault-Agent credential
> reconciliation, no audit sink, no FIPS. The production shape is
> `scenarios/cfk-openshift-linuxone/` plus the
> `accelerators/confluent-on-linuxone/` Kustomize layers. Do not point this at
> regulated data.

## Bill of materials

| Dimension | Value |
|---|---|
| Target architecture | IBM z16 (Telum), 4 chips |
| LPAR profile | Dedicated IFLs |
| Physical IFLs | **32** (BoM) → **34** with Connect |
| Central storage | **212 GB** (BoM) → **220 GB** with Connect |
| Storage fabric | FICON |
| Usable storage | **38.88 TB** (50 MB/s, 3d retention, RF 3) |
| Topology | OpenShift Container Platform |
| Workload | 3 brokers, 2 TaskManagers, 2 Connect workers |
| Consolidation | Kafka 8:1, Flink 2:1, HA control plane 4:1 |

### Where the 32 IFLs go

```
  Kafka tier          5   (3 brokers x 12 x86-ref vCPU / 8:1)
  Flink tier          8   (2 TaskManagers x 8 x86-ref vCPU / 2:1)
  Control plane      11   (KRaft + Schema Registry + Control Center + CMF)
  OCP platform tax    8   (6 static master/infra + 10% of workload)
  ----------------------
  BoM total          32
  Connect            +2   (locally estimated -- see below)
  ----------------------
  Actual ask         34
```

**Only 13 of the 32 IFLs are workload.** The other 19 are fixed overhead paid
once. That is where the headroom lives — not in spare engines, but in a cheap
marginal cost: roughly **+1 IFL per broker, +4 per TaskManager**. Storage is
the one genuinely slack dimension: 38.88 TB is ingest-driven and does not move
with broker count.

### Schema Registry is already paid for

SR sits inside the 11-IFL / 64 GB control-plane budget alongside KRaft,
Control Center, and CMF. It is not an unbudgeted addition.

### Connect is not

`z-stream-sizer` models exactly four control-plane components and **has no
Connect tier** — there is no occurrence of "connect" anywhere in `sizing.go`
or the UI. The +2 IFL / +8 GB figure here is a locally-derived demo-scale
estimate, flagged separately everywhere it appears. A production Connect tier
carrying real CDC volume is materially larger.

## The number that will bite you: x86-reference vCPU ≠ pod CPU

The sizer reports **12 vCPU per broker**. That is an *x86-reference* footprint
which it divides by the 8:1 consolidation ratio to reach physical IFLs. It is
**not** what goes in `resources.requests.cpu`.

Kubernetes schedules against the logical CPUs the LPAR exposes, and under
**SMT-2 one IFL presents two of them**:

```
Kafka tier:  36 x86-ref vCPU / 8:1 = 5 IFLs
             5 IFLs x 2 (SMT-2)     = 10 logical vCPU
             10 / 3 brokers         = 3.33 vCPU per broker pod
```

Requesting `cpu: 12` per broker would overcommit the Kafka tier ~3.6× and
leave pods `Pending`.

| Component | IFLs | Logical vCPU | Per pod |
|---|---|---|---|
| Broker ×3 | 5 | 10 | `3300m` cpu, `32Gi` |
| TaskManager ×2 | 8 | 16 | `8` cpu, `16Gi` |
| Connect ×2 | 2 | 4 | `2000m` cpu, `4Gi` |

The TaskManager figure coincidentally equals the sizer's x86 number because
the 2:1 Flink ratio and SMT-2 cancel. Do not generalise that to Kafka.

See `wiki/concepts/confluent-on-s390x-support-and-ifl-sizing.md`
("IFLs vs. vCPUs vs. Pods").

## Preconditions

Check these **before** starting, not after a failure.

1. **Kube context.** `oc config current-context` — confirm it is the intended
   cluster. Pass `--context` explicitly where you can.
2. **CP 8.2.0 is the s390x floor.** Earlier CP releases publish no s390x
   artifacts at all. Note that `scenarios/cfk-openshift/connectors/README.md`
   pins `cp-server-connect:7.6.0` for the x86 path — that image **cannot run
   here**. Use `Dockerfile.connect` in this directory.
3. **MongoDB on s390x.** MongoDB Community does not reliably publish s390x
   images. Verify before deploying:
   ```bash
   podman manifest inspect docker.io/library/mongo:7 | grep -i s390x
   ```
   If empty, run MongoDB on an x86 worker in a mixed-arch cluster (the default
   — `mongodb/mongodb-demo.yaml` sets no arch affinity), or point at an
   external MongoDB. Do **not** run it under QEMU emulation; the ~10×
   throughput penalty makes change-stream latency meaningless as a demo.
   The Confluent side is fine on Z — the MongoDB *connector* is pure Java, and
   per the support matrix only "connectors that rely on native OS libraries"
   are excluded.
4. **FICON storage class.** `oc get storageclass` — substitute the real name
   for `<PLACEHOLDER_FICON_STORAGE_CLASS>` in `values/kafka.yaml`.
5. **Ansible collections.** `ansible-galaxy collection install kubernetes.core`

## Deploy

```bash
# 0. Size-only. No cluster contact. Fails closed if the topology
#    does not fit the LPAR.
ansible-playbook ../../ansible/playbooks/deploy-linuxone-demo.yml \
  -i ../../ansible/inventories/linuxone-demo/hosts.yml --tags preflight

# 1. Secrets (demo-grade: plain Secrets, not Vault)
oc create namespace confluent || true
oc create secret generic mongodb-demo-root -n confluent \
  --from-literal=username=demo \
  --from-literal=password="$(openssl rand -base64 24)"

# 2. Build and push the s390x Connect image, then set the tag in
#    values/connect.yaml
podman build --platform linux/s390x -f Dockerfile.connect \
  -t <your-registry>/fsi-cp-connect-mongo:8.2.0-s390x .

# 3. Full deploy
ansible-playbook ../../ansible/playbooks/deploy-linuxone-demo.yml \
  -i ../../ansible/inventories/linuxone-demo/hosts.yml
```

Then create the Connect-facing Mongo secret once the root password is known:

```bash
oc create secret generic mongodb-demo-creds -n confluent \
  --from-literal=connection_uri="mongodb://demo:<pass>@mongodb-demo-0.mongodb-demo.confluent.svc.cluster.local:27017/?replicaSet=rs0"
```

## Demo flow

```
demo.customer (MongoDB)
  --[MongoSourceConnector, change streams]--> demo.customer.v1 (Kafka)
  --[Flink SQL on the session cluster]------> demo.customer.enriched.v1
  --[MongoSinkConnector]--------------------> demo.customer_enriched (MongoDB)
```

The sink writes to a **different collection** than the source reads on
purpose. Sinking back into `demo.customer` would feed the source's own change
stream and loop forever.

Drive it by inserting into Mongo and watching the round trip:

```bash
oc exec -n confluent mongodb-demo-0 -- mongosh --quiet \
  -u demo -p '<pass>' --authenticationDatabase admin \
  "mongodb://localhost:27017/?replicaSet=rs0" \
  --eval 'db.getSiblingDB("demo").customer.insertOne({name:"acme", tier:"gold"})'
```

## Verify

```bash
# Connect plugins actually loaded on s390x — both classes must appear
oc exec -n confluent connect-0 -- \
  curl -s localhost:8083/connector-plugins | \
  grep -o 'com.mongodb.kafka.connect.Mongo[A-Za-z]*Connector'

# Connectors running
oc get connector -n confluent -l app.kubernetes.io/part-of=fsi-linuxone-demo

# Nothing Pending (the symptom of getting the CPU translation wrong)
oc get pods -n confluent --field-selector=status.phase=Pending

# Confirm pods actually landed on s390x nodes
oc get pods -n confluent -o wide
```

## Files

| Path | Purpose |
|---|---|
| `values/kafka.yaml` | Brokers, KRaft quorum, Schema Registry — sized, s390x affinity |
| `values/connect.yaml` | Single demo Connect cluster with the budget warning |
| `flink/flink-session-cluster.yaml` | FKO session cluster, 2 TaskManagers |
| `connectors/mongodb-{source,sink}.yaml` | Demo-grade Connector CRs |
| `mongodb/mongodb-demo.yaml` | MongoDB StatefulSet + replica-set initiation Job |
| `topics/demo-topics.yaml` | KafkaTopic CRs including both DLQs |
| `Dockerfile.connect` | s390x Connect image, CP 8.2.0 + Mongo connector |

Ansible lives at `ansible/playbooks/deploy-linuxone-demo.yml`,
`ansible/roles/z_lpar_preflight/`, and
`ansible/inventories/linuxone-demo/`.
