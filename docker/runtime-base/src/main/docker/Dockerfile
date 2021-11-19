FROM deephaven/java-and-python:local-build as install-wheels

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install liblzo2-2 curl; \
    rm -rf /var/lib/apt/lists/*

ARG REQUIREMENTS=requirements.txt
COPY $REQUIREMENTS requirements.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r requirements.txt

ARG EXTRA_REQS
COPY $EXTRA_REQS extra-requirements.txt
# Note: by running both requirements in a single invocation, we'll make sure that there aren't any conflicting requirements
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r requirements.txt -r extra-requirements.txt; \
    rm requirements.txt; \
    rm extra-requirements.txt

ARG NO_INDEX=--no-index
COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q $NO_INDEX --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q $NO_INDEX --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q $NO_INDEX --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

FROM install-wheels as freeze
RUN set -eux; \
    mkdir /tmp/pip/; \
    python3 -m pip freeze > /tmp/pip/requirements.txt

FROM deephaven/java-and-python:local-build as runtime-base
COPY --from=install-wheels / /
