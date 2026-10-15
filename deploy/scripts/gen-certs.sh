#!/usr/bin/env bash
# Generates a private CA, TLS keystores for every Kafka node and the admin client,
# and random passwords, into deploy/secrets/. Both docker-compose.yml and the
# Kubernetes kustomization read from there.
#
#   ./scripts/gen-certs.sh            # create everything (refuses to overwrite)
#   ./scripts/gen-certs.sh --force    # throw away the old CA and start again
#
# This is a self-contained CA for running the stack. In a real environment, issue
# certificates from your organisation's PKI or cert-manager instead.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=secrets
DAYS=825
NAMESPACE=${NAMESPACE:-kafka}

if [[ -e "$OUT/ca.crt" && "${1:-}" != "--force" ]]; then
  echo "$OUT/ already has a CA. Re-run with --force to replace all certificates and passwords." >&2
  exit 1
fi
rm -rf "$OUT"
mkdir -p "$OUT"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

password() { openssl rand -base64 32 | tr -d '/+=' | cut -c1-32; }

NODE_KEYSTORE_PASSWORD=$(password)
ADMIN_KEYSTORE_PASSWORD=$(password)
APP_SCRAM_PASSWORD=$(password)

echo "==> CA"
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -subj "/O=kafka-in-practice/CN=kafka-in-practice CA" \
  -keyout "$OUT/ca.key" -out "$OUT/ca.crt" 2>/dev/null

# issue <name> <subject> <extendedKeyUsage> <subjectAltName or ""> <keystore password>
issue() {
  local name=$1 subject=$2 eku=$3 san=$4 pass=$5
  local ext="$work/$name.ext"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$eku"
    [[ -n "$san" ]] && echo "subjectAltName=$san"
  } > "$ext"
  openssl req -new -newkey rsa:2048 -nodes -subj "$subject" \
    -keyout "$work/$name.key" -out "$work/$name.csr" 2>/dev/null
  openssl x509 -req -sha256 -days "$DAYS" -in "$work/$name.csr" \
    -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
    -extfile "$ext" -out "$work/$name.crt" 2>/dev/null
  # PKCS12 keystore holding the key, its certificate and the CA chain.
  openssl pkcs12 -export -name "$name" \
    -inkey "$work/$name.key" -in "$work/$name.crt" -certfile "$OUT/ca.crt" \
    -passout "pass:$pass" -out "$OUT/$name.p12"
  echo "    $OUT/$name.p12"
}

# Every node certificate has OU=kafka-nodes. ssl.principal.mapping.rules turns all of
# them into the single principal User:kafka-node, which is a super user. Brokers and
# controllers need both serverAuth and clientAuth because they connect to each other.
echo "==> Docker Compose node certificates"
for node in controller-1 controller-2 controller-3 broker-1 broker-2 broker-3; do
  issue "$node" "/O=kafka-in-practice/OU=kafka-nodes/CN=$node" \
    "serverAuth,clientAuth" "DNS:$node,DNS:localhost" "$NODE_KEYSTORE_PASSWORD"
done

echo "==> Kubernetes node certificate (shared by all pods, wildcard SANs)"
k8s_sans="DNS:*.kafka-controller-headless.$NAMESPACE.svc.cluster.local"
k8s_sans+=",DNS:*.kafka-controller-headless.$NAMESPACE.svc"
k8s_sans+=",DNS:*.kafka-broker-headless.$NAMESPACE.svc.cluster.local"
k8s_sans+=",DNS:*.kafka-broker-headless.$NAMESPACE.svc"
k8s_sans+=",DNS:kafka.$NAMESPACE.svc.cluster.local,DNS:kafka.$NAMESPACE.svc,DNS:localhost"
issue kafka-node "/O=kafka-in-practice/OU=kafka-nodes/CN=kafka-node" \
  "serverAuth,clientAuth" "$k8s_sans" "$NODE_KEYSTORE_PASSWORD"

echo "==> Admin client certificate (principal User:admin, a super user)"
issue admin "/O=kafka-in-practice/OU=kafka-admins/CN=admin" "clientAuth" "" "$ADMIN_KEYSTORE_PASSWORD"

# Read by the brokers and controllers through FileConfigProvider.
cat > "$OUT/node-credentials.properties" <<EOF
node.keystore.password=$NODE_KEYSTORE_PASSWORD
EOF

# Read by the admin tools and the init job only.
cat > "$OUT/admin-credentials.properties" <<EOF
admin.keystore.password=$ADMIN_KEYSTORE_PASSWORD
app.scram.password=$APP_SCRAM_PASSWORD
EOF

# The CA key signs certificates; nothing in the cluster needs it. Keep it offline.
mkdir -p "$OUT/ca-private"
mv "$OUT/ca.key" "$OUT/ca.srl" "$OUT/ca-private/" 2>/dev/null || true
chmod 700 "$OUT/ca-private"
chmod 600 "$OUT/ca-private/"*
# Containers run as a different uid than you, so they need read access to the rest.
# On a real host, restrict this with ownership (chown to the container uid) instead.
chmod 755 "$OUT"
chmod 644 "$OUT"/*.p12 "$OUT"/*.properties "$OUT/ca.crt"

echo
echo "Done. Client password for user 'app' is in $OUT/admin-credentials.properties (app.scram.password)."
