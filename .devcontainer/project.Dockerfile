# Project-specific layer for deephaven-core, built on top of the upstream
# claude-code-devcontainer base image (see ./Dockerfile, fetched by devc.sh).
#
# Keep this file separate from the fetched Dockerfile so upstream stays easy
# to diff/refresh; add project tooling here instead.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# deephaven-core's dev toolchain (bootstrap JDK, Node, Python + native build
# toolchain, C++ client build deps) is described once, in the repo's
# ../flake.nix, and built here via Nix rather than duplicated as hand-written
# apt-get lines. See ../flake.nix's top comment for what's in `devEnv` and
# why it only pins a *bootstrap* JDK: Gradle's own toolchain
# auto-provisioning (org.gradle.toolchains.foojay-resolver-convention)
# still downloads whatever per-subproject JDK (11-25) a given build target
# needs, so pinning the full version range here too would just be a second,
# competing source of truth for the same versions.
USER root
RUN mkdir -m 0755 /nix && chown vscode:vscode /nix
USER vscode

# Single-user, no-daemon install: this base image has no systemd, so the
# default multi-user (daemon) Nix install isn't available here.
RUN curl -fsSL https://nixos.org/nix/install | sh -s -- --no-daemon
ENV PATH="/home/vscode/.nix-profile/bin:${PATH}"
RUN mkdir -p /home/vscode/.config/nix && \
  echo "experimental-features = nix-command flakes" >> /home/vscode/.config/nix/nix.conf

# Copied into a plain (non-git) directory deliberately: a flake source
# tree that lives inside a Git working tree must have every file checked
# in (`git add`) before Nix will read it, which doesn't apply here. nix/
# holds the flake's own local modules (gradle-wrapper.nix, podman.nix)
# that flake.nix imports, so it has to come along too -- devc.sh's
# cmd_build stages all three (flake.nix, flake.lock, nix/) into this
# build context before running `podman build`.
COPY --chown=vscode:vscode flake.nix flake.lock /home/vscode/deephaven-flake/
COPY --chown=vscode:vscode nix/ /home/vscode/deephaven-flake/nix/

# --out-link produces a *stable* path (not content-hashed), so it's safe to
# hardcode below in ENV -- unlike the /nix/store/<hash>-... path it resolves
# to, which changes whenever flake.lock's pins change.
RUN nix build /home/vscode/deephaven-flake#devEnv --out-link /opt/nix-dev-env

# Baked as plain image ENV -- not sourced from .zshrc -- because devc.sh's
# `exec` runs `podman exec ... "$@"` directly with no shell in between, so
# .zshrc is never sourced there (only `devc.sh shell`'s interactive zsh
# would see it). Static ENV is the one mechanism both commands share.
ENV PATH="/opt/nix-dev-env/bin:${PATH}"
ENV JAVA_HOME="/opt/nix-dev-env"
