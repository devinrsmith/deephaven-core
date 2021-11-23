FROM deephaven/runtime-base:local-build as install

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install liblzo2-2 curl; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    GRPC_HEALTH_PROBE_VERSION=v0.3.1; \
    curl -L -o /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    chmod +x /bin/grpc_health_probe

COPY licenses/ /

ARG DEEPHAVEN_VERSION

ADD distributions/server-${DEEPHAVEN_VERSION}.tar /opt/deephaven

RUN set -eux; \
    ln -s /opt/deephaven/server-${DEEPHAVEN_VERSION} /opt/deephaven/server

FROM deephaven/runtime-base:local-build

COPY --from=install / /
VOLUME /data
VOLUME /cache
# Breakdown of the following command:
#   * Wait 3 seconds before checking the status of the container after each test.
#   * After three failing checks, mark the container as failed.
#   * The HEALTHCHECK will wait 11 seconds for a result, but the grpc_health_probe process
#     will time out after only 10 seconds of trying to connect. This one second leaves room
#     for the grpc probe to have a delay starting up, and gives some visiblity into this
#     failure mode (via docker inspect, but only until that failure stales out).
#   * If grpc_health_probe fails with any non-zero exit code, normalize to 1.
# The net effect is that the server will have 3s until the earliest possible success, and
# 33-36s until it is considered to have failed startup (so downstream docker-compose services
# will not attempt to start). If the server is running but trying to shut down, failure will
# occur within about 9s, as the port will connect correctly, but respond that it isn't
# available.
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT [ "/opt/deephaven/server/bin/start" ]
