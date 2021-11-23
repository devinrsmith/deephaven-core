FROM deephaven/server-slim:local-build as install

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install \
      python3.7 \
      libpython3.7 \
      python3-pip \
      python3-setuptools \
      python3-wheel \
      ; \
    rm -rf /var/lib/apt/lists/*

# To update the requirements.txt:
# 1. Comment out BLOCK 1, and uncomment BLOCK 2
# 2. Run `./gradlew docker-java-and-python:build`
# 3. Run `docker run --rm -it deephaven/java-and-python:local-build pip freeze --all > docker/java-and-python/src/main/docker/requirements.txt`
# 4. Uncomment BLOCK 1, and comment out BLOCK 2

# START BLOCK 1

COPY system-requirements.txt system-requirements.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r system-requirements.txt; \
    rm system-requirements.txt

COPY requirements.txt requirements.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r requirements.txt; \
    rm requirements.txt
# END BLOCK 1

# START BLOCK 2
#RUN set -eux; \
#    python3 -m pip install --no-cache-dir --upgrade pip; \
#    python3 -m pip install --no-cache-dir --upgrade setuptools; \
#    python3 -m pip install --no-cache-dir --upgrade wheel
# END BLOCK 2

COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

FROM deephaven/server-slim:local-build
COPY --from=install / /
