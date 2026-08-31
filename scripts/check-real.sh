#!/usr/bin/env bash
# Only creates/stops its own disposable container. No existing Redis address is accepted.
set -euo pipefail
cd "$(dirname "$0")/.."
image="${1:?usage: bash scripts/check-real.sh redis:7-alpine|valkey/valkey:8}"
case "$image" in
  redis:*) server=redis-server; cli=redis-cli ;;
  valkey/valkey:*) server=valkey-server; cli=valkey-cli ;;
  *) echo "expected a Redis or Valkey image" >&2; exit 1 ;;
esac
container_id=$(docker create --label distsync.test=integration --publish 127.0.0.1::6379 "$image" "$server" --save '' --appendonly no)
cleanup() {
  docker rm --force --volumes "$container_id" >/dev/null
  echo "Removed own integration container $container_id and disposable volumes"
}
trap cleanup EXIT
docker start "$container_id" >/dev/null
ready=0
for ((i=0; i<100; i++)); do
  if docker exec "$container_id" "$cli" ping >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.1
done
if [[ "$ready" != 1 ]]; then echo "isolated server did not start" >&2; exit 1; fi
address=$(docker port "$container_id" 6379/tcp)
case "$address" in 127.0.0.1:*) ;; *) echo "not loopback: $address" >&2; exit 1 ;; esac
echo "Own integration server $image at $address ($container_id)"
export GOWORK=off
export DISTSYNC_TEST_REDIS_ADDR="$address"
# Fault cases create separate instances: killing them cannot damage this server.
export DISTSYNC_FAULT_IMAGE="$image"
go test ./... -count=1 -timeout 300s -v
go test ./... -race -count=1 -timeout 300s -v
