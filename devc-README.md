# devc.sh

A rootless-Podman runner for Claude Code's sandboxed dev container, with no
Node/npm and no Docker dependency.

## Purpose

We want to run Claude Code with `--dangerously-skip-permissions` safely,
which requires isolating the whole process (filesystem, not just Bash) in a
container. [Trail of Bits' `claude-code-devcontainer`](https://github.com/trailofbits/claude-code-devcontainer)
provides a good `Dockerfile` + `devcontainer.json` for this, but their
tooling assumes:

- the `devcontainer` CLI (`npm install -g @devcontainers/cli`), and
- a Docker-compatible engine (Docker Desktop, Colima, OrbStack).

Neither fits this environment: no npm install desired on the host, and
Podman (rootless, no root access) is the only container engine available.

`devc.sh` re-implements the relevant parts of Trail of Bits'
`devcontainer.json` as direct `podman build` / `podman run` invocations, so
the same sandboxed environment runs with only Podman and `bash` on the host.

## What it does NOT do

- **No automatic network firewall.** The upstream `devcontainer.json` grants
  `NET_ADMIN`/`NET_RAW` capabilities but does not run iptables rules by
  default — that's documented upstream as a manual, optional step. This
  script preserves that: capabilities are granted, but no egress
  restriction is applied unless you add it yourself (see upstream README's
  "Network Isolation" section for the allowlist pattern).
- **No IDE integration.** This is a plain CLI tool. There's no VS Code /
  Cursor "Reopen in Container" equivalent — you build, start, and shell in
  from the terminal.
- **No devcontainer.json parsing.** The relevant fields from upstream's
  `devcontainer.json` (`runArgs`, `mounts`, `containerEnv`, `remoteEnv`,
  `postCreateCommand`) are hand-translated into this script. If upstream
  changes their `devcontainer.json`, **this script must be updated to
  match** — it does not read that file at all, only `Dockerfile`,
  `.zshrc`, and `post_install.py`.

## How it works

1. **Fetches upstream build assets.** On first `build`/`up`, downloads
   `Dockerfile`, `.zshrc`, and `post_install.py` from the upstream repo into
   `.devcontainer/` if not already present. These are cached locally after
   that — no repeat network calls unless you delete them.

2. **Builds the image with `podman build`,** passing `TZ` as the one build
   arg the Dockerfile expects. (Upstream's `devcontainer.json` also declares
   a `github-cli` "feature" — a devcontainer-spec-only concept with no
   Podman equivalent. It's currently dropped; see "Adding project-specific
   tools" below if you need `gh` or anything else.)

3. **Runs the container with `podman run -d ... sleep infinity`,** keeping
   it alive in the background so `shell`/`exec` can attach to it repeatedly
   without rebuilding. Flags map 1:1 from upstream's `devcontainer.json`:

   | devcontainer.json field | podman flag |
   |---|---|
   | `runArgs` (`NET_ADMIN`, `NET_RAW`) | `--cap-add` |
   | `mounts` (named volumes) | `-v name:path` |
   | `mounts` (bind mounts) | `-v host:container:ro,Z` (adds `:Z` for SELinux — Docker doesn't need this, Podman on Fedora does) |
   | `containerEnv` / `remoteEnv` | `-e` |
   | `postCreateCommand` | run once via `podman exec` right after container creation |

4. **Runs as a non-root user via `--userns=keep-id`.** The upstream base
   image (`mcr.microsoft.com/devcontainers/base:ubuntu24.04`) creates its
   `vscode` user at UID/GID 1000. `keep-id` remaps that to your actual host
   UID/GID, so files the container writes into the bind-mounted workspace
   are owned by you on the host, not an unreachable subuid. **If a future
   base image changes that UID, update `CONTAINER_UID`/`CONTAINER_GID` at
   the top of the script to match** — check with `podman exec -u vscode
   <container> id`.

5. **Forces `--cgroup-manager=cgroupfs`** via a shell function that shadows
   the `podman` command everywhere in the script. Rootless Podman's default
   systemd cgroup manager needs a D-Bus session with lingering enabled,
   which requires root to set up — cgroupfs sidesteps that dependency
   entirely, at the cost of systemd-based resource-limit enforcement (not
   used here anyway).

6. **Names everything per-project**, derived from the current directory
   name (lowercased, non-alphanumeric → `-`, leading/trailing `-` trimmed
   via bash parameter expansion). This lets multiple projects each get
   their own image, container, and volumes without colliding.

## Commands

```
./devc.sh build      Build (or rebuild) the sandbox image
./devc.sh up         Create + start the container (idempotent)
./devc.sh shell      Open an interactive zsh shell in the container
./devc.sh exec CMD   Run a one-off command in the container
./devc.sh stop       Stop the container (keeps volumes/image)
./devc.sh destroy [--purge-auth] [--purge-gradle]
                      Remove container + image (+ bash-history volume).
                      Claude/gh login and Gradle cache volumes are kept
                      unless --purge-auth / --purge-gradle is passed.
./devc.sh status     Show what's running
```

## Adding project-specific tools

`Dockerfile`, `.zshrc`, and `post_install.py` in `.devcontainer/` are fetched
verbatim from upstream and meant to stay diffable against it — don't add
project tooling there. Instead, drop an optional `.devcontainer/project.Dockerfile`
into the repo (checked into git, unlike the fetched files):

```dockerfile
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends <your-package> \
  && apt-get clean && rm -rf /var/lib/apt/lists/*
USER vscode
```

`cmd_build` builds the fetched `Dockerfile` as `<image>-base` first, then — if
`project.Dockerfile` exists — builds it `FROM` that base and tags the result
as `<image>`; otherwise `<image>-base` is tagged directly as `<image>`. This
keeps the upstream mirror untouched while letting each repo layer on
whatever it needs.

deephaven-core's own `project.Dockerfile` uses this to install [Nix](https://nixos.org)
and build the dev toolchain described in the repo's `flake.nix` (bootstrap
JDK, Node, Python + native build toolchain, C++ client build deps) — see
`flake.nix`'s top comment for why it's the single source of truth for those
versions instead of hand-written `apt-get`/`nix-env` lines. `cmd_build`
stages copies of `flake.nix`/`flake.lock` into `.devcontainer/` before this
build, since `project.Dockerfile`'s build context is `.devcontainer/`, not
the repo root (see the `cp` call in `cmd_build`). The resulting toolchain is
exposed as image-level `ENV PATH`/`JAVA_HOME`, not sourced from `.zshrc`,
because `devc.sh exec` runs `podman exec ... "$@"` with no shell in
between — only image `ENV` is visible to both `shell` and `exec` alike.

The same `flake.nix` also works standalone, independent of this
Claude-Code-specific sandbox — see its top comment for `nix develop`
usage if you just want a reproducible shell to build deephaven-core in,
without any of the Claude Code / sandboxing tooling below.

## Claude/gh login persistence

Your Claude Code login (`~/.claude/.credentials.json`, from `claude login`)
and `gh` login both live in named Podman volumes (`devc-<project>-claude-config`,
`devc-<project>-gh-config`), not in the container or image. That means:

- `./devc.sh build` (image rebuild) never touches them — login survives.
- `podman rm`-ing the container and re-running `./devc.sh up` never touches
  them either — login survives, since volumes outlive the container.
- `./devc.sh destroy` also leaves them alone by default — pass
  `--purge-auth` if you actually want to force a fresh login (e.g. rotating
  a token, or the auth got corrupted).

If a login still doesn't survive a rebuild/re-create, check that you didn't
run `destroy --purge-auth`, and that nothing outside devc.sh removed the
volumes (`podman volume ls`, `podman volume prune`, a host reset).

## Gradle cache persistence

`~/.gradle` (Gradle's default `GRADLE_USER_HOME`) holds the dependency
cache, the wrapper's downloaded distribution, auto-provisioned JDK
toolchains, and the build daemon/state — all expensive to rebuild, and
this repo's build downloads a lot of it (a full sync is 1GB+). It's backed
by the `devc-<project>-gradle` named volume, mounted the same way as the
Claude/gh config volumes, so it survives `build`/`up`/`stop`/plain
container re-creation, and is kept by default on `destroy` too. Pass
`destroy --purge-gradle` if you want to force a clean Gradle state (e.g.
you suspect cache corruption).

## Docker API access for Testcontainers / gradle-docker-plugin builds

Some of deephaven-core's Gradle tasks need a real Docker-API endpoint, not
just an image registry:

- `testOutOfBand` in `extensions/kafka`, `extensions/iceberg/s3`,
  `extensions/parquet/table`, `extensions/s3`, etc. use **Testcontainers**
  to spin up Kafka/Redpanda/MinIO/LocalStack.
- The `:docker-*` subprojects and `buildSrc/.../Docker.groovy` use the
  **bmuschko gradle-docker-plugin**, which talks to the Docker API directly
  (via `docker-java`) to build/tag/run images.

Both are plain Java clients that speak the Docker Engine API over a
Unix socket — they don't need the `docker` or `podman` CLI installed
inside the sandbox, just a socket and `DOCKER_HOST` pointing at it.

`devc.sh` can expose the **host's** rootless Podman API socket into the
container for this (Podman speaks a Docker-compatible API on the same
socket). It's off by default; opt in with:

```bash
DEVC_DOCKER_API=1 ./devc.sh up
```

This binds the host's `$XDG_RUNTIME_DIR/podman/podman.sock` into the
container at the *same path* (required — the API server resolves
bind-mount sources like Testcontainers' Ryuk-reaper socket mount against
its own, host-side filesystem) and sets `DOCKER_HOST`/
`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` accordingly. `ensure_podman_socket`
starts `podman system service` on the host automatically if it isn't
already listening — no `systemctl --user enable podman.socket` needed.

**Security tradeoff — read before enabling.** This is Docker-outside-of-
Docker: containers created through that socket are **siblings on the
host**, not nested inside the sandbox. Anything running inside the
container — including Claude Code under `--dangerously-skip-permissions`
— can use it to launch a container as your host user with an arbitrary
bind mount (e.g. your home directory) and read/write/execute anything that
user can. That's a real reduction of the isolation this whole script
exists to provide, so only set `DEVC_DOCKER_API=1` when you actually need
to run the Docker-dependent build/test tasks, not as a standing default.

Known rough edge: Testcontainers' Ryuk reaper has had issues cleaning up
under rootless Podman. If it hangs or fails, try
`-e TESTCONTAINERS_RYUK_DISABLED=true` (leftover containers then need
manual `podman ps -a` / `podman rm` cleanup on the host instead).

## Known rough edges to watch for

- **`SHELL is not supported for OCI image format`** warning during build is
  expected and harmless — it just means the Dockerfile's `pipefail` safety
  net is ignored for that one `RUN` line. Add `--format docker` to the
  `podman build` call in `cmd_build` if this ever needs to be respected.
- **First build is slow** — the Dockerfile installs a full Node/Python/uv
  toolchain plus Claude Code itself. This is expected, not a script bug.
- **Firewall is opt-in only** (see above) — don't assume network isolation
  exists unless you've explicitly added iptables rules.
- **Upstream drift**: since this script hand-translates
  `devcontainer.json` rather than parsing it, periodically diff this
  script's flags against the latest upstream `devcontainer.json` at
  https://github.com/trailofbits/claude-code-devcontainer/blob/main/devcontainer.json
  to catch any new fields that need translating.

## Editing this script

- Keep the `podman()` shell-function override in place — it forces
  `--cgroup-manager=cgroupfs` on every call, which is what lets this run
  without a systemd session or root.
- If you add new mounts or env vars, add them to both `cmd_up`'s
  `mount_args`/`env_args` arrays and this README's mapping table.
- If you add automatic firewall support, document the exact allowlist
  clearly since it directly affects the security guarantees users are
  relying on this script for.
- Project-specific tools go in `.devcontainer/project.Dockerfile`, not the
  fetched `Dockerfile` — see "Adding project-specific tools" above.
