#!/usr/bin/env bash
# Entrypoint for both controllers and brokers, in Docker Compose and Kubernetes.
#
# 1. Renders /etc/kafka/templates/<KAFKA_ROLE>.properties into a server.properties,
#    replacing every @@NAME@@ with the environment variable NAME.
# 2. Formats the storage directory on first start (a no-op afterwards).
# 3. Replaces itself with the Kafka server, so signals reach the JVM directly and
#    a SIGTERM triggers a controlled shutdown.
#
# Required: KAFKA_ROLE (controller|broker), CLUSTER_ID, QUORUM_VOTERS, and whatever
# placeholders the template uses. NODE_ID may be omitted on Kubernetes: it is then
# derived from the StatefulSet ordinal (pod name suffix) plus NODE_ID_OFFSET.
set -euo pipefail

: "${KAFKA_ROLE:?KAFKA_ROLE must be controller or broker}"
: "${CLUSTER_ID:?CLUSTER_ID must be set (generate one with kafka-storage.sh random-uuid)}"

if [[ -z "${NODE_ID:-}" ]]; then
  ordinal=${HOSTNAME##*-}
  [[ $ordinal =~ ^[0-9]+$ ]] || { echo "Cannot derive NODE_ID from hostname '$HOSTNAME'" >&2; exit 1; }
  export NODE_ID=$(( ordinal + ${NODE_ID_OFFSET:-0} ))
fi

template=${KAFKA_TEMPLATE_DIR:-/etc/kafka/templates}/$KAFKA_ROLE.properties
runtime_dir=${KAFKA_RUNTIME_DIR:-/etc/kafka/runtime}
config=$runtime_dir/server.properties
mkdir -p "$runtime_dir"

# Substitute @@NAME@@ placeholders. ${...} is left alone on purpose: it is the syntax
# of Kafka's config providers, which Kafka resolves itself at startup.
while IFS= read -r line || [[ -n $line ]]; do
  while [[ $line =~ @@([A-Z0-9_]+)@@ ]]; do
    name=${BASH_REMATCH[1]}
    if [[ -z "${!name+set}" ]]; then
      echo "Template $template needs environment variable $name" >&2
      exit 1
    fi
    line=${line//"@@$name@@"/"${!name}"}
  done
  printf '%s\n' "$line"
done < "$template" > "$config"

echo "===> $KAFKA_ROLE node.id=$NODE_ID, config rendered to $config"

/opt/kafka/bin/kafka-storage.sh format \
  --cluster-id "$CLUSTER_ID" \
  --config "$config" \
  --ignore-formatted

exec /opt/kafka/bin/kafka-server-start.sh "$config"
