FROM deephaven/java:local-build as install

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install liblzo2-2 curl; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    GRPC_HEALTH_PROBE_VERSION=v0.3.1; \
    curl -L -o /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    chmod +x /bin/grpc_health_probe

ARG DEEPHAVEN_VERSION

ADD distributions/server-${DEEPHAVEN_VERSION}.tar /opt/deephaven

RUN set -eux; \
    ln -s /opt/deephaven/server-${DEEPHAVEN_VERSION} /opt/deephaven/server

FROM deephaven/java:local-build
COPY --from=install / /
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT [ "/opt/deephaven/server/bin/start" ]