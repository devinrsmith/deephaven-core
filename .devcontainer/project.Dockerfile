# Project-specific layer for deephaven-core, built on top of the upstream
# claude-code-devcontainer base image (see ./Dockerfile, fetched by devc.sh).
#
# Keep this file separate from the fetched Dockerfile so upstream stays easy
# to diff/refresh; add project tooling here instead.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# deephaven-core is a Gradle multi-project build. The Gradle wrapper needs a
# JVM on PATH just to bootstrap itself (Gradle 9.7.1, this repo's wrapper
# version, requires Java 17+ to launch) — install one JDK for that. Gradle's
# own toolchain auto-provisioning (org.gradle.toolchains.foojay-resolver-convention)
# then downloads whatever JDK a given subproject actually targets (11-25),
# so there's no need to pre-install the full version range here.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
  openjdk-21-jdk-headless \
  && apt-get clean && rm -rf /var/lib/apt/lists/*
USER vscode
