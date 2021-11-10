# https://www.envoyproxy.io/docs/envoy/latest/start/docker#build-and-run-a-docker-image
ARG BASE
FROM $BASE
COPY contents/ /
RUN set -eux; \
    mv /envoy.yaml /etc/envoy/envoy.yaml; \
    chmod go+r /etc/envoy/envoy.yaml
