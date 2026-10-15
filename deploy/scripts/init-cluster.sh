#!/usr/bin/env bash
# One-shot cluster setup, run as the admin super user over mTLS:
#   - the SCRAM user the application logs in with
#   - the application's topics
#   - ACLs that give the application exactly what it needs and nothing more
#
# Safe to run again: every step is create-if-missing or an idempotent update.
set -euo pipefail

: "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS must point at the INTERNAL listener of the brokers}"
ADMIN_CONFIG=${ADMIN_CONFIG:-/etc/kafka/admin/admin.properties}
CREDENTIALS=${CREDENTIALS:-/etc/kafka/secrets/admin-credentials.properties}
APP_USER=${APP_USER:-app}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-3}
MIN_ISR=${MIN_ISR:-2}

bin=${KAFKA_HOME:-/opt/kafka}/bin
admin=(--bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_CONFIG")

property() { grep -E "^$1=" "$CREDENTIALS" | head -1 | cut -d= -f2-; }

echo "===> Waiting for the brokers"
until $bin/kafka-broker-api-versions.sh "${admin[@]}" > /dev/null 2>&1; do
  sleep 5
done

echo "===> SCRAM credentials for User:$APP_USER"
app_password=$(property app.scram.password)
$bin/kafka-configs.sh "${admin[@]}" --alter --entity-type users --entity-name "$APP_USER" \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=$app_password]"

echo "===> Topics"
create_topic() {
  local topic=$1 partitions=$2; shift 2
  $bin/kafka-topics.sh "${admin[@]}" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor "$REPLICATION_FACTOR" \
    --config min.insync.replicas="$MIN_ISR" "$@"
}
# Keyed by customer. 12 partitions divide evenly over 1, 2, 3, 4, 6 and 12 consumers.
create_topic orders 12
# Failed records from the billing consumer; kept longer so they can be inspected and replayed.
create_topic orders.billing.dlq 3 --config retention.ms=2592000000

echo "===> ACLs for User:$APP_USER"
acl() { $bin/kafka-acls.sh "${admin[@]}" --add --allow-principal "User:$APP_USER" "$@"; }
acl --producer --topic orders                    # ProduceOrders (idempotent producer)
acl --consumer --topic orders --group billing    # ConsumeOrders
acl --producer --topic orders.billing.dlq        # dead-lettered records

$bin/kafka-acls.sh "${admin[@]}" --list --principal "User:$APP_USER"
echo "===> Cluster initialised"
