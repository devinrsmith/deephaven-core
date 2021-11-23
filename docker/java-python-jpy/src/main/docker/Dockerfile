FROM deephaven/java-and-python:local-build as install-wheels

COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

FROM deephaven/java-and-python:local-build
COPY --from=install-wheels / /