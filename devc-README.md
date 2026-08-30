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
./devc.sh destroy    Remove container + this project's volumes + image
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
whatever it needs (a JDK for deephaven-core's Gradle build, `gh`, etc.).

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
