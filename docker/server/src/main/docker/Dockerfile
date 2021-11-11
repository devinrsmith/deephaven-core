ARG BASE

FROM deephaven/deephaven-jpy-wheel:local-build as deephaven-jpy-wheel
FROM $BASE as install

COPY common/ common/
ARG TARGETARCH
RUN set -eux; \
    ./common/server-requirements.sh; \
    rm -r ./common

COPY --from=deephaven-jpy-wheel /usr/src/app/dist /deephaven-jpy
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven-jpy/*.whl; \
    rm -r /deephaven-jpy

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

ADD server/server.tar /opt/deephaven

FROM $BASE
COPY --from=install / /
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT /opt/deephaven/grpc-api-server-native-0.6.0/bin/grpc-api-server-native