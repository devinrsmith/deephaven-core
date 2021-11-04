ARG BASE=ghcr.io/devinrsmith/java-python
FROM $BASE as install
COPY install.sh install.sh
ARG TARGETARCH
RUN ./install.sh

FROM $BASE
COPY --from=install / /
