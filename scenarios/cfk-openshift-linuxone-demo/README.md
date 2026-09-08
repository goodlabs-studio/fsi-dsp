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

> **Scope: the Confluent side only.** MongoDB is provisioned and owned by a
> separate team. Nothing here stands it up — this scenario owns the Kafka /
> Flink / Connect estate and the two Connector CRs that talk to their
> instance. See [The MongoDB dependency](#the-mongodb-dependency).

## Bill of materials

| Dimension | Value |
|---|---|
| Target architecture | IBM z16 (Telum), 4 chips |
| LPAR profile | Dedicated IFLs |
| Physical IFLs | **32** — fixed, Connect fits inside |
| Central storage | **212 GB** — 207 used, 5 GB spare |
| Storage fabric | FICON |
| Usable storage | **38.88 TB** (50 MB/s, 3d retention, RF 3) |
| Topology | OpenShift Container Platform |
| Workload | 3 brokers, 2 TaskManagers, 2 Connect workers |
| Consolidation | Kafka 8:1, Flink 2:1, HA control plane 4:1 |

### Where the 32 IFLs go

```
  Kafka tier          4   (3 brokers x 10 x86-ref vCPU / 8:1)   <- trimmed
  Flink tier          8   (2 TaskManagers x 8 x86-ref vCPU / 2:1)
  Control plane      11   (KRaft + Schema Registry + Control Center + CMF)
  OCP platform tax    8   (6 static master/infra + 10% of workload)
  Connect             1   (2 workers x 1000m, locally estimated)
  ----------------------
  TOTAL              32   exactly the allocation
```

**Only 12 of the 32 IFLs are workload.** The other 20 are fixed overhead paid
once. That is where the headroom lives — not in spare engines, but in a cheap
marginal cost: roughly **+1 IFL per broker, +4 per TaskManager**. Storage is
the one genuinely slack dimension: 38.88 TB is ingest-driven and does not move
with broker count.

### How Connect was fitted without raising the ask

At the sizer's default 12 x86-ref vCPU per broker the Confluent estate
consumes **all 32 IFLs and all 212 GB with zero spare**, and Connect — which
`z-stream-sizer` does not model at all — had nowhere to go. The LPAR
allocation is fixed, so the **broker tier was trimmed instead**:

| | Broker x86-ref vCPU | Kafka tier | Estate | Broker pod |
|---|---|---|---|---|
| Sizer default | 12 | 5 IFLs | 32 IFL / 212 GB (0 spare) | 3333m / 32Gi |
| **This demo** | **10** | **4 IFLs** | **31 IFL / 199 GB** | **2666m / 28Gi** |
| + Connect | | +1 IFL | **32 IFL / 207 GB** | |

Brokers are the least-loaded component in a MongoDB CDC demo, so the capacity
comes from where it is least missed. **Do not carry this trim into a
production sizing** — re-run the sizer.

Note that two Connect workers at 1000m cost the *same one IFL* as one worker
at 2000m, because SMT-2 rounding makes `ceil(2x1/2)` and `ceil(1x2/2)`
identical. Two workers therefore buy restart survivability for 4 GB of RAM and
zero IFLs.

### Schema Registry is already paid for

SR sits inside the 11-IFL / 64 GB control-plane budget alongside KRaft,
Control Center, and CMF. It is not an unbudgeted addition.

### Connect is not

`z-stream-sizer` models exactly four control-plane components and **has no
Connect tier** — there is no occurrence of "connect" anywhere in `sizing.go`
or the UI. The 1 IFL / 8 GB allocated here is a locally-derived demo-scale
estimate, flagged separately everywhere it appears. A production Connect tier
carrying real CDC volume is materially larger.

## The number that will bite you: x86-reference vCPU ≠ pod CPU

The sizer reports **12 vCPU per broker**. That is an *x86-reference* footprint
which it divides by the 8:1 consolidation ratio to reach physical IFLs. It is
**not** what goes in `resources.requests.cpu`.

Kubernetes schedules against the logical CPUs the LPAR exposes, and under
**SMT-2 one IFL presents two of them**:

```
Kafka tier:  30 x86-ref vCPU / 8:1 = 4 IFLs
             4 IFLs x 2 (SMT-2)     = 8 logical vCPU
             8 / 3 brokers          = 2.67 vCPU per broker pod
```

Requesting `cpu: 10` (let alone the sizer's 12) per broker would overcommit
the tier several times over and leave pods `Pending`.

| Component | IFLs | Logical vCPU | Per pod |
|---|---|---|---|
| Broker ×3 | 4 | 8 | `2666m` cpu, `28Gi` |
| TaskManager ×2 | 8 | 16 | `8` cpu, `16Gi` |
| Connect ×2 | 1 | 2 | `1000m` cpu, `4Gi` |

Round pod requests **down** into the tier: three brokers at 2667m would ask
for 8001m against 8000m and the third would never schedule.

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
3. **MongoDB handover.** Owned by another team — see
   [The MongoDB dependency](#the-mongodb-dependency) for the four-point
   contract. Create the Secret from the connection string they provide:
   ```bash
   oc create secret generic mongodb-demo-creds -n confluent \
     --from-literal=connection_uri='mongodb://<user>:<pass>@<host>:27017/?replicaSet=<rs>'
   ```
   `--tags mongodb` verifies it before anything depends on it.
4. **FICON storage class.** `oc get storageclass` — substitute the real name
   for `<PLACEHOLDER_FICON_STORAGE_CLASS>` in `values/kafka.yaml`.
5. **Ansible collections.** `ansible-galaxy collection install kubernetes.core`

## Deploy

```bash
# 0. Size-only. No cluster contact. Fails closed if the topology
#    does not fit the LPAR.
ansible-playbook ../../ansible/playbooks/deploy-linuxone-demo.yml \
  -i ../../ansible/inventories/linuxone-demo/hosts.yml --tags preflight

# 1. Namespace + the MongoDB connection secret, from the string the
#    MongoDB team provides (demo-grade: plain Secret, not Vault)
oc create namespace confluent || true
oc create secret generic mongodb-demo-creds -n confluent \
  --from-literal=connection_uri='mongodb://<user>:<pass>@<host>:27017/?replicaSet=<rs>'

# 1b. Verify their handover before building anything on it
ansible-playbook ../../ansible/playbooks/deploy-linuxone-demo.yml \
  -i ../../ansible/inventories/linuxone-demo/hosts.yml --tags mongodb

# 2. Build and push the s390x Connect image, then set the tag in
#    values/connect.yaml
podman build --platform linux/s390x -f Dockerfile.connect \
  -t <your-registry>/fsi-cp-connect-mongo:8.2.0-s390x .

# 3. Full deploy
ansible-playbook ../../ansible/playbooks/deploy-linuxone-demo.yml \
  -i ../../ansible/inventories/linuxone-demo/hosts.yml
```

## Demo flow

```
demo.customer (MongoDB, other team's instance)
  --[MongoSourceConnector, change streams]--> demo.customer.v1 (Kafka)
  --[MongoSinkConnector]--------------------> demo.customer_enriched (MongoDB)
```

**No Flink in the data path, deliberately.** The Flink session cluster *is*
deployed here — it is in the BoM at 2 TaskManagers / 8 IFLs — but it comes up
idle. Stream processing belongs to the **fraud detection demo**, which submits
its own SQL against this cluster. Pointing the sink at `demo.customer.v1`
directly means this scenario demonstrates a complete CDC round trip on its own
instead of waiting on a job another demo owns. When the fraud demo adds a
Flink hop, repoint `topics` in `connectors/mongodb-sink.yaml` at whatever that
job produces.

Note an idle session cluster still holds its 8 IFLs — scale-to-zero is a
Confluent Cloud behaviour, not an FKO one. If the fraud demo is far off and
you want those engines back, drop `taskManager.replicas` and re-run
`--tags preflight` to confirm the new totals.

The sink writes to a **different collection** than the source reads on
purpose. Sinking back into `demo.customer` would feed the source's own change
stream and loop forever.

Drive it by inserting into `demo.customer` on the MongoDB team's instance —
from wherever you normally reach it — and watching the round trip land:

```javascript
// against their mongosh / client
db.getSiblingDB("demo").customer.insertOne({ name: "acme", tier: "gold" })
```

```bash
# watch it arrive on the Kafka side
oc exec -n confluent kafka-0 -- kafka-console-consumer \
  --bootstrap-server localhost:9071 \
  --topic demo.customer.v1 --from-beginning --max-messages 5
```

## The MongoDB dependency

**MongoDB is stood up and owned by another team.** Nothing in this scenario
provisions it, and no MongoDB manifests ship here — only the two Connector CRs
that read from and write to their instance.

For the record: MongoDB *is* supported on LinuxONE under the MongoDB/IBM OEM
agreement, so it can run in-frame beside Confluent. There is no *community*
s390x manifest (`docker.io/library/mongo` is x86/arm only) because the Z build
is distributed through IBM — but that is the MongoDB team's problem to solve,
not a constraint on this scenario.

### The contract — confirm all four before demo day

1. **It is a replica set, not a standalone.** MongoDB change streams — the
   mechanism the source connector uses — do not exist on a standalone
   `mongod`. This is the one that bites: everything deploys cleanly and then
   fails at the first change-stream read with
   `The $changeStream stage is only supported on replica sets`.
2. **The connection URI carries `replicaSet=<name>`.**
3. **Database `demo`, collection `customer`** exists and is writable, and
   **`customer_enriched` is free** for the sink to own.
4. **Credentials** grant change-stream/read on `demo.customer` and write on
   `demo.customer_enriched`.

Points 1 and 2 are asserted automatically — `--tags mongodb` decodes the
Secret and fails with a message naming the gap. Points 3 and 4 are a
conversation, not a check.

### Sizing note

The MongoDB pods are **outside** the 32-IFL BoM — `z-stream-sizer` models the
Confluent estate only. Whether they share this LPAR or get their own is the
MongoDB team's capacity question, not a charge against these 32 IFLs.

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
| `topics/demo-topics.yaml` | KafkaTopic CRs including both DLQs |
| `Dockerfile.connect` | s390x Connect image, CP 8.2.0 + Mongo connector |

Ansible lives at `ansible/playbooks/deploy-linuxone-demo.yml`,
`ansible/roles/z_lpar_preflight/`, and
`ansible/inventories/linuxone-demo/`.
