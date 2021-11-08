# deephaven.server-slim.baseImage
ARG BASE

FROM $BASE as install

COPY common/ common/
ARG TARGETARCH
RUN set -eux; \
    ./common/server-requirements.sh; \
    rm -r ./common

ADD server/server.tar /opt/deephaven

FROM $BASE
COPY --from=install / /
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT /opt/deephaven/grpc-api-server-native-0.6.0/bin/grpc-api-server-native