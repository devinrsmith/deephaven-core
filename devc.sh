#!/usr/bin/env bash
#
# devc.sh — rootless-Podman runner for Trail of Bits' claude-code-devcontainer
# config, without the devcontainer CLI (npm) or Docker.
#
# Reads Dockerfile / .zshrc / post_install.py from ./.devcontainer/, fetching
# them from the upstream repo on first run if missing. Translates the
# upstream devcontainer.json's runArgs/mounts/env into podman flags directly.
#
# Usage:
#   ./devc.sh build      Build (or rebuild) the sandbox image
#   ./devc.sh up         Create + start the container (idempotent)
#   ./devc.sh shell       Open an interactive zsh shell in the container
#   ./devc.sh exec CMD    Run a one-off command in the container
#   ./devc.sh stop        Stop the container (keeps volumes/image)
#   ./devc.sh destroy [--purge-auth] [--purge-gradle]
#                         Remove container + image (+ bash-history volume).
#                         Claude/gh login and Gradle cache volumes are kept
#                         unless --purge-auth / --purge-gradle is given.
#   ./devc.sh status      Show what's running
#
# Env vars:
#   DEVC_DOCKER_API=1     On `up`, expose the host's rootless Podman API
#                         socket inside the container (Docker-outside-of-
#                         Docker) for Testcontainers / gradle-docker-plugin
#                         builds. Weakens sandbox isolation — see
#                         devc-README.md before enabling.
#
set -euo pipefail

# ---- Configuration -----------------------------------------------------

UPSTREAM_RAW="https://raw.githubusercontent.com/trailofbits/claude-code-devcontainer/main"
DEVCONTAINER_DIR=".devcontainer"
PROJECT_NAME="$(basename "$(pwd)")"
PROJECT_NAME="${PROJECT_NAME,,}"            # lowercase (bash 4+)
PROJECT_NAME="${PROJECT_NAME//[^a-z0-9]/-}" # non-alnum -> hyphen
PROJECT_NAME="${PROJECT_NAME#-}"            # trim leading hyphen
PROJECT_NAME="${PROJECT_NAME%-}"            # trim trailing hyphen
IMAGE_NAME="claude-sandbox-${PROJECT_NAME}"
CONTAINER_NAME="claude-sandbox-${PROJECT_NAME}"

# The upstream base image creates its non-root user at UID/GID 1000.
# --userns=keep-id maps that to *your* host UID/GID so bind-mounted files
# (like the workspace) don't end up owned by an unreachable subuid.
CONTAINER_UID=1000
CONTAINER_GID=1000

VOL_HISTORY="devc-${PROJECT_NAME}-bashhistory"
VOL_CLAUDE_CONFIG="devc-${PROJECT_NAME}-claude-config"
VOL_GH_CONFIG="devc-${PROJECT_NAME}-gh-config"
VOL_GRADLE="devc-${PROJECT_NAME}-gradle"

WORKSPACE_HOST="$(pwd)"
WORKSPACE_CONTAINER="/workspace"

# Opt-in: expose the *host's* rootless Podman API socket inside the
# container (Docker-outside-of-Docker), for Gradle tasks that need a
# Docker-compatible API (Testcontainers-based `testOutOfBand` tests, the
# bmuschko gradle-docker-plugin's DockerBuildImage tasks). This is a real
# reduction in the sandbox's isolation: anything running inside the
# container — including Claude under --dangerously-skip-permissions — can
# use that socket to launch *sibling* containers on the host as your host
# user, which can bind-mount and touch anything that user can. Off by
# default; set DEVC_DOCKER_API=1 to enable it. See devc-README.md.
DEVC_DOCKER_API="${DEVC_DOCKER_API:-0}"
PODMAN_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"

# ---- Helpers -------------------------------------------------------------

log() { printf '\033[1;34m[devc]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[devc]\033[0m %s\n' "$*" >&2; exit 1; }

require_podman() {
  command -v podman >/dev/null 2>&1 || die "podman not found on PATH."
}

# Rootless Podman defaults to the systemd cgroup manager, which needs a
# logind session with lingering enabled (root-only to set up) to talk to
# systemd over D-Bus. Force cgroupfs instead so nothing here needs systemd
# or root at all.
podman() { command podman --cgroup-manager=cgroupfs "$@"; }

fetch_devcontainer_files() {
  mkdir -p "$DEVCONTAINER_DIR"
  local files=("Dockerfile" ".zshrc" "post_install.py")
  for f in "${files[@]}"; do
    if [[ ! -f "$DEVCONTAINER_DIR/$f" ]]; then
      log "Fetching $f from upstream..."
      curl -fsSL "$UPSTREAM_RAW/$f" -o "$DEVCONTAINER_DIR/$f" \
        || die "Failed to fetch $f. Place it manually in $DEVCONTAINER_DIR/."
    fi
  done
}

# Starts `podman system service` on the host if its API socket isn't
# already listening at $PODMAN_SOCK. Only called when DEVC_DOCKER_API=1.
# Deliberately doesn't rely on `systemctl --user enable podman.socket`
# (socket activation), matching this script's no-systemd-dependency stance
# elsewhere (see the cgroupfs note above) — it's spawned directly instead.
ensure_podman_socket() {
  if [[ -S "$PODMAN_SOCK" ]] && podman --url "unix://${PODMAN_SOCK}" info >/dev/null 2>&1; then
    return 0
  fi
  log "Starting host Podman API service at ${PODMAN_SOCK}..."
  mkdir -p "$(dirname "$PODMAN_SOCK")"
  nohup podman system service --time=0 "unix://${PODMAN_SOCK}" >/dev/null 2>&1 &
  disown
  local i
  for ((i = 0; i < 40; i++)); do
    [[ -S "$PODMAN_SOCK" ]] && return 0
    sleep 0.25
  done
  die "Podman API socket did not come up at ${PODMAN_SOCK}."
}

container_exists() {
  podman container exists "$CONTAINER_NAME" 2>/dev/null
}

container_running() {
  [[ "$(podman inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || echo false)" == "true" ]]
}

# ---- Commands --------------------------------------------------------

cmd_build() {
  require_podman
  fetch_devcontainer_files

  log "Building base image ${IMAGE_NAME}-base..."
  podman build \
    --build-arg "TZ=${TZ:-UTC}" \
    -t "${IMAGE_NAME}-base" \
    -f "$DEVCONTAINER_DIR/Dockerfile" \
    "$DEVCONTAINER_DIR"

  # Project-specific tooling (e.g. a JDK for this repo's Gradle build) lives
  # in an optional, repo-tracked project.Dockerfile layered on top of the
  # upstream-fetched base, rather than editing the fetched Dockerfile.
  local project_dockerfile="${DEVCONTAINER_DIR}/project.Dockerfile"
  if [[ -f "$project_dockerfile" ]]; then
    log "Building project image ${IMAGE_NAME}..."
    podman build \
      --build-arg "BASE_IMAGE=${IMAGE_NAME}-base" \
      -t "$IMAGE_NAME" \
      -f "$project_dockerfile" \
      "$DEVCONTAINER_DIR"
  else
    podman tag "${IMAGE_NAME}-base" "$IMAGE_NAME"
  fi

  log "Build complete."
}

cmd_up() {
  require_podman

  if container_running; then
    log "Container already running."
    return 0
  fi

  if container_exists; then
    log "Starting existing container..."
    podman start "$CONTAINER_NAME" >/dev/null
    return 0
  fi

  if ! podman image exists "$IMAGE_NAME"; then
    log "Image not found, building first..."
    cmd_build
  fi

  # Ensure a gitconfig exists to bind-mount read-only (upstream does the same
  # no-op touch so the mount never fails on a fresh machine).
  [[ -f "$HOME/.gitconfig" ]] || touch "$HOME/.gitconfig"

  local mount_args=(
    -v "${VOL_HISTORY}:/commandhistory"
    -v "${VOL_CLAUDE_CONFIG}:/home/vscode/.claude"
    -v "${VOL_GH_CONFIG}:/home/vscode/.config/gh"
    -v "${VOL_GRADLE}:/home/vscode/.gradle"
    -v "${HOME}/.gitconfig:/home/vscode/.gitconfig:ro,Z"
    -v "${WORKSPACE_HOST}/${DEVCONTAINER_DIR}:${WORKSPACE_CONTAINER}/${DEVCONTAINER_DIR}:ro,Z"
    -v "${WORKSPACE_HOST}:${WORKSPACE_CONTAINER}:Z"
  )

  # Optional: bind .git/config and .git/hooks read-only, matching upstream,
  # but only if this workspace is actually a git repo.
  if [[ -f "${WORKSPACE_HOST}/.git/config" ]]; then
    mount_args+=(-v "${WORKSPACE_HOST}/.git/config:${WORKSPACE_CONTAINER}/.git/config:ro,Z")
  fi
  if [[ -d "${WORKSPACE_HOST}/.git/hooks" ]]; then
    mount_args+=(-v "${WORKSPACE_HOST}/.git/hooks:${WORKSPACE_CONTAINER}/.git/hooks:ro,Z")
  fi

  if [[ "$DEVC_DOCKER_API" == "1" ]]; then
    ensure_podman_socket
    # Bind at the identical path on both sides: the API server resolves
    # bind-mount sources (e.g. Testcontainers' Ryuk mounting "the docker
    # socket" into itself) against its own (host) filesystem, so the path
    # has to already be valid there too.
    mount_args+=(-v "${PODMAN_SOCK}:${PODMAN_SOCK}:Z")
  fi

  local env_args=(
    -e "NODE_OPTIONS=--max-old-space-size=4096"
    -e "CLAUDE_CONFIG_DIR=/home/vscode/.claude"
    -e "POWERLEVEL9K_DISABLE_GITSTATUS=true"
    -e "GIT_CONFIG_GLOBAL=/home/vscode/.gitconfig.local"
    -e "UV_LINK_MODE=copy"
    -e "NPM_CONFIG_IGNORE_SCRIPTS=true"
    -e "NPM_CONFIG_AUDIT=true"
    -e "NPM_CONFIG_FUND=false"
    -e "NPM_CONFIG_SAVE_EXACT=true"
    -e "NPM_CONFIG_UPDATE_NOTIFIER=false"
    -e "NPM_CONFIG_MIN_RELEASE_AGE=1"
    -e "PYTHONDONTWRITEBYTECODE=1"
    -e "PIP_DISABLE_PIP_VERSION_CHECK=1"
    -e "CLAUDE_CODE_OAUTH_TOKEN=${CLAUDE_CODE_OAUTH_TOKEN:-}"
    -e "ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY:-}"
  )

  if [[ "$DEVC_DOCKER_API" == "1" ]]; then
    env_args+=(
      -e "DOCKER_HOST=unix://${PODMAN_SOCK}"
      -e "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=${PODMAN_SOCK}"
    )
  fi

  log "Creating container ${CONTAINER_NAME}..."
  podman run -d \
    --name "$CONTAINER_NAME" \
    --userns="keep-id:uid=${CONTAINER_UID},gid=${CONTAINER_GID}" \
    --cap-add=NET_ADMIN \
    --cap-add=NET_RAW \
    --workdir "$WORKSPACE_CONTAINER" \
    "${mount_args[@]}" \
    "${env_args[@]}" \
    "$IMAGE_NAME" \
    sleep infinity

  log "Running post-install..."
  podman exec -u vscode "$CONTAINER_NAME" \
    bash -lc 'uv run --no-project /opt/post_install.py' || true

  log "Container is up. Run './devc.sh shell' to enter it."
}

cmd_shell() {
  require_podman
  container_running || cmd_up
  podman exec -it -u vscode -w "$WORKSPACE_CONTAINER" "$CONTAINER_NAME" zsh
}

cmd_exec() {
  require_podman
  [[ $# -gt 0 ]] || die "Usage: ./devc.sh exec <command...>"
  container_running || cmd_up
  podman exec -it -u vscode -w "$WORKSPACE_CONTAINER" "$CONTAINER_NAME" "$@"
}

cmd_stop() {
  require_podman
  container_exists || { log "No container to stop."; return 0; }
  podman stop "$CONTAINER_NAME" >/dev/null
  log "Stopped."
}

cmd_destroy() {
  require_podman

  local purge_auth=false
  local purge_gradle=false
  for arg in "$@"; do
    case "$arg" in
      --purge-auth) purge_auth=true ;;
      --purge-gradle) purge_gradle=true ;;
      *) die "Unknown option for destroy: $arg" ;;
    esac
  done

  if container_exists; then
    podman rm -f "$CONTAINER_NAME" >/dev/null
    log "Removed container."
  fi

  # VOL_CLAUDE_CONFIG and VOL_GH_CONFIG hold logged-in auth state (Claude's
  # .credentials.json, gh's config) that's expensive to redo interactively.
  # VOL_GRADLE holds the Gradle dependency/wrapper/toolchain cache, expensive
  # to redownload. Keep them all by default so a routine destroy/up cycle
  # doesn't force a re-login or a from-scratch Gradle sync; only bash
  # history is always disposable.
  podman volume rm -f "$VOL_HISTORY" >/dev/null 2>&1 || true
  if [[ "$purge_auth" == true ]]; then
    for v in "$VOL_CLAUDE_CONFIG" "$VOL_GH_CONFIG"; do
      podman volume rm -f "$v" >/dev/null 2>&1 || true
    done
    log "Removed auth volumes (--purge-auth)."
  fi
  if [[ "$purge_gradle" == true ]]; then
    podman volume rm -f "$VOL_GRADLE" >/dev/null 2>&1 || true
    log "Removed Gradle cache volume (--purge-gradle)."
  fi

  if podman image exists "$IMAGE_NAME"; then
    podman rmi -f "$IMAGE_NAME" >/dev/null
    log "Removed image."
  fi

  local kept=()
  [[ "$purge_auth" == true ]] || kept+=("Claude/gh login")
  [[ "$purge_gradle" == true ]] || kept+=("Gradle cache")
  case "${#kept[@]}" in
    0) log "Destroyed everything for this project, including auth and Gradle cache." ;;
    1) log "Destroyed container/image for this project (${kept[0]} preserved; use --purge-auth / --purge-gradle to wipe it too)." ;;
    *) log "Destroyed container/image for this project (${kept[0]} and ${kept[1]} preserved; use --purge-auth / --purge-gradle to wipe them too)." ;;
  esac
}

cmd_status() {
  require_podman
  if container_running; then
    log "Running: $CONTAINER_NAME"
  elif container_exists; then
    log "Stopped: $CONTAINER_NAME"
  else
    log "Not created: $CONTAINER_NAME"
  fi
  podman volume ls --filter "name=devc-${PROJECT_NAME}" --format '  volume: {{.Name}}' 2>/dev/null || true
}

# ---- Entry point -------------------------------------------------------

case "${1:-}" in
  build)   cmd_build ;;
  up)      cmd_up ;;
  shell)   cmd_shell ;;
  exec)    shift; cmd_exec "$@" ;;
  stop)    cmd_stop ;;
  destroy) shift; cmd_destroy "$@" ;;
  status)  cmd_status ;;
  *)
    cat >&2 <<EOF
Usage: $0 <command>

  build      Build the sandbox image
  up         Create and start the container
  shell      Open a zsh shell inside the container
  exec CMD   Run a command inside the container
  stop       Stop the container
  destroy [--purge-auth] [--purge-gradle]
             Remove container, image, and bash-history volume for this
             project. Claude/gh login and Gradle cache volumes are kept
             unless --purge-auth / --purge-gradle is given, which also
             wipes them.
  status     Show current state
EOF
    exit 1
    ;;
esac
