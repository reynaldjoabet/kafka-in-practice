# Deploying Kafka

A production-style Apache Kafka 4.4 cluster, packaged two ways from one set of config files:

- **Docker Compose** (`docker-compose.yml`) for a single machine.
- **Kubernetes** (`kustomization.yaml` + `k8s/`) for a real cluster.

Both run the official `apache/kafka` image in KRaft mode, with no ZooKeeper.

## What you get

```mermaid
flowchart TB
    subgraph Q["Controller quorum (3 voters)"]
        C1["controller 1"] --- C2["controller 2"] --- C3["controller 3"]
    end
    subgraph B["Brokers (racks a, b, c)"]
        B1["broker 101"]
        B2["broker 102"]
        B3["broker 103"]
    end
    B1 & B2 & B3 -- "CONTROLLER :9093<br/>mutual TLS" --> Q
    B1 <-- "INTERNAL :9091<br/>mutual TLS" --> B2
    B2 <-- "INTERNAL :9091" --> B3
    App["Applications<br/>user: app"] -- "CLIENT :9092 / EXTERNAL<br/>SCRAM-SHA-512 over TLS" --> B
    Admin["Admin tools<br/>user: admin"] -- "INTERNAL :9091<br/>mutual TLS" --> B
```

| Area | Setting | Why |
|---|---|---|
| Topology | 3 dedicated controllers, 3 brokers | Survives the loss of any one controller and any one broker |
| Durability | `default.replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false` | An acknowledged write is on at least 2 brokers; no data-losing leader elections |
| Internal topics | Offsets, transactions and share-group state at RF 3 / min ISR 2 | The defaults are fine, but this makes them explicit |
| Topic creation | `auto.create.topics.enable=false` | Topics are created on purpose, with chosen partitions and retention |
| Encryption | TLS on every listener (TLS 1.3 between servers) | Nothing travels in plaintext |
| Server identity | Mutual TLS between controllers and brokers | A node must present a certificate signed by the cluster CA |
| Client identity | SASL/SCRAM-SHA-512 on the client listeners, re-authenticated hourly | Passwords never cross the wire; removed users lose access |
| Authorization | `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false` | Default deny: a user can do only what an ACL allows |
| Secrets | Keystore passwords read through `FileConfigProvider` | No passwords in config files or environment variables |
| Rack awareness | `broker.rack` + `RackAwareReplicaSelector` | Replicas spread across racks; consumers can read from the nearest replica |
| Container | Non-root, read-only root filesystem, controlled shutdown on SIGTERM | Smaller attack surface, no log recovery after a normal restart |

### Listeners

| Listener | Port | Protocol | Who uses it |
|---|---|---|---|
| `CONTROLLER` | 9093 | SSL, client certificate required | Controllers among themselves, brokers to controllers |
| `INTERNAL` | 9091 | SSL, client certificate required | Broker replication, admin tools |
| `CLIENT` | 9092 | SASL_SSL (SCRAM-SHA-512) | Applications on the same network |
| `EXTERNAL` | 19092 / 29092 / 39092 | SASL_SSL (SCRAM-SHA-512) | Applications on your machine (Compose only) |

### Principals

| Principal | How it authenticates | Rights |
|---|---|---|
| `User:kafka-node` | Any certificate with `OU=kafka-nodes` (every broker and controller) | Super user |
| `User:admin` | The `admin.p12` client certificate | Super user |
| `User:app` | SCRAM password | Only the ACLs below |

`User:app` can produce to `orders`, consume `orders` in group `billing`, and produce to `orders.billing.dlq`. This matches what `ProduceOrders` and `ConsumeOrders` in this project need. Change the topics and ACLs in `scripts/init-cluster.sh`.

## Layout

```
deploy/
├── docker-compose.yml          Docker Compose entry point
├── .env                        image, cluster id, memory and thread sizes for Compose
├── kustomization.yaml          Kubernetes entry point (kubectl apply -k deploy/)
├── k8s/                        StatefulSets, Services, PDBs, NetworkPolicies, init Job
├── config/
│   ├── controller.properties   controller config template
│   ├── broker.properties       broker config template
│   └── admin.properties        admin client config (mTLS as User:admin)
├── scripts/
│   ├── gen-certs.sh            creates the CA, keystores and passwords in secrets/
│   ├── kafka-entrypoint.sh     renders the template, formats storage, starts Kafka
│   └── init-cluster.sh         creates the SCRAM user, topics and ACLs
└── secrets/                    generated, git-ignored
```

The config templates contain placeholders written as a name between two `@@` markers, such as `NODE_ID`. `kafka-entrypoint.sh` fills them from environment variables, so the same files serve every node on both platforms. Only per-node values (node id, listeners, rack) come from the environment.

## 1. Generate certificates and passwords

Needs only `openssl`.

```bash
cd deploy
./scripts/gen-certs.sh
```

This creates `secrets/`:

| File | Contents | Used by |
|---|---|---|
| `ca.crt` | CA certificate (truststore, PEM) | Everyone, including your applications |
| `controller-{1,2,3}.p12`, `broker-{1,2,3}.p12` | One keystore per node | Docker Compose |
| `kafka-node.p12` | One keystore with wildcard names for all pods | Kubernetes |
| `admin.p12` | Admin client certificate | Admin tools, init job |
| `node-credentials.properties` | Node keystore password | Brokers, controllers |
| `admin-credentials.properties` | Admin keystore password, the `app` user's SCRAM password | Admin tools, init job |
| `ca-private/ca.key` | CA private key | Nothing at runtime. Keep it offline. |

`secrets/` is in `.gitignore`. Never commit it.

## 2a. Run with Docker Compose

```bash
cd deploy
docker compose up -d
docker compose logs -f kafka-init     # SCRAM user, topics and ACLs; ends with "Cluster initialised"
docker compose ps
```

The memory and heap in `.env` are sized so six nodes fit on a laptop (about 18 GB in total). On real hosts, raise `BROKER_HEAP` to `-Xms6g -Xmx6g` and `BROKER_MEMORY` to at least 16g. The memory above the heap is the OS page cache, which serves most reads.

### Connect an application from your machine

```properties
bootstrap.servers=localhost:19092,localhost:29092,localhost:39092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="app" password="<app.scram.password from secrets/admin-credentials.properties>";
ssl.truststore.type=PEM
ssl.truststore.location=/absolute/path/to/deploy/secrets/ca.crt
```

In this project, pass these through `KafkaConnection.security`:

```scala
val connection = KafkaConnection(
  bootstrapServers = "localhost:19092,localhost:29092,localhost:39092",
  clientId = "orders-producer",
  security = Map(
    "security.protocol"       -> "SASL_SSL",
    "sasl.mechanism"          -> "SCRAM-SHA-512",
    "sasl.jaas.config"        -> s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="app" password="$password";""",
    "ssl.truststore.type"     -> "PEM",
    "ssl.truststore.location" -> "deploy/secrets/ca.crt"
  )
)
```

The examples in `Orders.scala` currently build a `KafkaConnection` without `security`, so they need this change to talk to this cluster.

### Admin commands

The brokers do not hold the admin certificate. Run tools from the init container, which does:

```bash
docker compose run --rm --entrypoint bash kafka-init
# inside:
B=broker-1:9091 C=/etc/kafka/admin/admin.properties
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config $C --describe --topic orders
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $B --command-config $C --describe --group billing
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $B --command-config $C describe --status
```

### Stop and reset

```bash
docker compose down        # keep data
docker compose down -v     # delete all data volumes too
```

## 2b. Run on Kubernetes

```bash
cd deploy
./scripts/gen-certs.sh          # if not done already; NAMESPACE=... to use another namespace
kubectl apply -k .
kubectl -n kafka get pods -w
kubectl -n kafka logs -f job/kafka-init
```

What is deployed:

| Resource | Purpose |
|---|---|
| `StatefulSet/kafka-controller` (3) + headless Service | Controller quorum, node ids 1–3, 10 Gi volume each |
| `StatefulSet/kafka-broker` (3) + headless Service | Brokers, node ids 101–103, 200 Gi volume each, 16 Gi memory, 6 GB heap |
| `Service/kafka` | Bootstrap address for applications: `kafka.kafka.svc:9092` |
| `PodDisruptionBudget` × 2 | Node drains never take down more than one controller or one broker at a time |
| `NetworkPolicy` × 2 | Controllers reachable only from Kafka pods; brokers' client port only from labelled namespaces |
| `Job/kafka-init` | Creates the SCRAM user, topics and ACLs |
| ConfigMaps and Secrets | Generated from `config/`, `scripts/` and `secrets/`. Their names carry a content hash, so changing a file rolls the pods. |

Pods run as uid 1000 with no capabilities, a read-only root filesystem and the `RuntimeDefault` seccomp profile, which meets the `restricted` Pod Security Standard the namespace enforces. Required anti-affinity puts each controller and each broker on a different node, so a cluster needs at least 3 nodes.

### Connect an application in the cluster

Label the application's namespace so the NetworkPolicy lets it in:

```bash
kubectl label namespace my-app kafka-access=true
```

Then use `bootstrap.servers=kafka.kafka.svc:9092` with the same SASL_SSL settings as above, and mount `ca.crt` (for example by copying the `kafka-node-tls` secret's `ca.crt` key into your namespace).

Brokers advertise their pod DNS names, so `kubectl port-forward` is not enough for clients outside the cluster. For external access, add an `EXTERNAL` listener per broker behind a LoadBalancer or NodePort Service and advertise those addresses.

### Admin commands

```bash
kubectl -n kafka run kafka-admin --rm -it --restart=Never --image=apache/kafka:4.4.0 \
  --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":1000,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"kafka-admin","image":"apache/kafka:4.4.0","command":["bash"],"stdin":true,"tty":true,"securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}},"volumeMounts":[{"name":"tls","mountPath":"/etc/kafka/secrets"},{"name":"cfg","mountPath":"/etc/kafka/admin"}]}],"volumes":[{"name":"tls","secret":{"secretName":"<kafka-admin-tls-... name>"}},{"name":"cfg","configMap":{"name":"<kafka-admin-config-... name>"}}]}}'
# get the generated names with: kubectl -n kafka get secret,configmap
```

## How it was verified

Docker was not running when this was written, so the stack has not yet been started with `docker compose up` or on a Kubernetes cluster. These checks were run:

- `docker compose config` and `kubectl kustomize` both succeed, and the generated names are wired into the workloads.
- The keystores made by OpenSSL open in Java's `keytool`, with the expected SANs and key usages.
- `ssl.principal.mapping.rules` was run through Kafka's own `SslPrincipalMapper`: node certificates map to `kafka-node`, and the admin certificate maps to `admin`.
- The rendered controller and broker configs pass `KafkaConfig` validation from Kafka 4.4.0, with the `${file:...}` secrets resolved.
- A real controller and broker were started from Kafka 4.4.0 jars with the rendered templates (changed only in ports, paths and replication factor 1). Over mutual TLS, the broker registered and was unfenced.
- The unmodified `init-cluster.sh` was run against that cluster (with `REPLICATION_FACTOR=1 MIN_ISR=1`). It created the user, the topics and the ACLs, and running it twice was safe.
- As `app` over SCRAM, producing to `orders` and consuming as group `billing` worked. A wrong password, a topic without ACLs, and a group without ACLs were all rejected with the right exceptions.
- SIGTERM led to a controlled shutdown.

## Before real production

These are deliberately left to your environment:

| Topic | What to do |
|---|---|
| Certificates | Issue them from your PKI or cert-manager instead of `gen-certs.sh`, with automatic rotation. Kafka can reload keystores without a restart through dynamic config. |
| Secrets | Store them in a secret manager (Vault, External Secrets, Sealed Secrets), not in files next to the manifests. |
| Rack | On Kubernetes, `BROKER_RACK` is the pod name. Set it to the node's zone (`topology.kubernetes.io/zone`), for example with an init container that reads the node label through the API, so replicas really spread across zones. |
| Storage | Set an SSD-backed `storageClassName`, and size volumes with the formulas in the main README (section 23). |
| Cluster id | Generate a new one per cluster (`kafka-storage.sh random-uuid`) and set it in `.env` and `kustomization.yaml`. |
| Monitoring | Add the Prometheus JMX exporter as a Java agent and alert on the metrics in the main README (section 31). JMX is disabled here because it has no authentication. |
| Quotas | Set produce/consume quotas per user so one application cannot starve the others. |
| External access | Add a per-broker external listener (see above). |
| Upgrades | Roll controllers first, then brokers, then finalise with `kafka-features.sh upgrade --release-version`. |
| Backups / DR | Mirror critical topics to a second cluster with MirrorMaker 2. |
