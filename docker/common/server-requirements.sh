#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

# Note: if the base image changes from debian:buster-slim to a more minimal image, we may need to modify the logic here
# to be more explicit about our dependencies.

apt-get -qq update
apt-get -qq -y --no-install-recommends install \
  curl \
  liblzo2-2 # needed for LZO parquet encoding

GRPC_HEALTH_PROBE_VERSION=v0.3.1
curl --silent \
  --location \
  --output /bin/grpc_health_probe \
  "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}"
chmod +x /bin/grpc_health_probe

apt-get -qq -y purge curl
apt-get -qq -y autoremove
rm -rf /var/lib/apt/lists/*