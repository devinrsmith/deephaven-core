# Provisions a rootless Podman stack via Nix and wires it up as a
# Docker-compatible API socket, for deephaven-core's use of the
# gradle-docker-plugin and Testcontainers -- see devc-README.md's "Docker
# API access for Testcontainers / gradle-docker-plugin builds" section,
# which this mirrors (same socket path, same --cgroup-manager=cgroupfs
# workaround, same DOCKER_HOST/TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE env
# vars) but for a bare `nix develop` shell instead of the Claude-Code
# sandbox container devc.sh builds.
#
# What Nix can and can't provide here, confirmed by inspecting the actual
# built package rather than assumed:
#   - pkgs.podman bundles its own OCI runtime (crun/runc), conmon, network
#     backend (netavark/aardvark-dns), and passt/pasta under
#     $out/libexec/podman/ -- confirmed via `find`/`strings` on the built
#     derivation. No separate buildInputs needed for those.
#   - Rootless containers still need HOST-level newuidmap/newgidmap
#     (SUID-root binaries, normally from the shadow-utils/shadow package)
#     and configured /etc/subuid, /etc/subgid entries. Nix cannot provide
#     or substitute for these on a non-NixOS host -- confirmed by testing:
#     pkgs.podman's own `podman info` fails immediately with "newuidmap:
#     executable file not found in $PATH" without them. The shellHook
#     below checks for this explicitly up front and warns clearly instead
#     of leaving the (harder to diagnose) failure to surface later, deep
#     inside a gradle-docker-plugin/Testcontainers stack trace.
#   - Registry/trust config (/etc/containers/registries.conf, policy.json)
#     isn't bundled by nixpkgs' podman either -- like any distro-installed
#     podman, it reads these from their normal host-absolute paths. If
#     your host already has podman/containers-common installed (this
#     repo's devc.sh already requires it), these already exist and
#     nothing extra is needed; a minimal fallback is written to a
#     Nix-shell-scoped location only when they're missing, same
#     prefer-what-already-exists approach as gradle-wrapper.nix's
#     isolatedHomeHook.
#
# NOT independently verified end-to-end against a real container run: the
# sandbox this was developed in is itself a nested Podman container
# without newuidmap/newgidmap at all (demonstrated above), so actual
# `podman run` / gradle-docker-plugin / Testcontainers behavior needs
# confirming on a real host.
{ pkgs }:
{
  extraBuildInputs = [ pkgs.podman ];

  shellHook = ''
    _podman_sock="''${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"

    # Rootless Podman defaults to the systemd cgroup manager, which needs a
    # logind session with lingering enabled (root-only to set up) to talk
    # to systemd over D-Bus. Force cgroupfs instead, same fix devc.sh
    # already applies for the same reason -- see its own comment on this.
    podman() { command podman --cgroup-manager=cgroupfs "$@"; }

    if ! command -v newuidmap >/dev/null 2>&1 || ! command -v newgidmap >/dev/null 2>&1; then
      echo "podman: newuidmap/newgidmap not found on PATH -- rootless Podman needs" >&2
      echo "  these from your host (shadow-utils/shadow package). Docker-API" >&2
      echo "  wiring skipped; gradle-docker-plugin/Testcontainers tasks needing" >&2
      echo "  it will not work in this shell." >&2
    elif [[ ! -s /etc/subuid || ! -s /etc/subgid ]]; then
      echo "podman: /etc/subuid or /etc/subgid is empty -- rootless Podman needs" >&2
      echo "  subordinate UID/GID ranges configured for your user (your distro's" >&2
      echo "  podman/shadow-utils install normally does this). Docker-API wiring" >&2
      echo "  skipped." >&2
    else
      _podman_conf_dir="''${XDG_CONFIG_HOME:-$HOME/.config}/containers"
      if [[ ! -s /etc/containers/policy.json && ! -s "$_podman_conf_dir/policy.json" ]]; then
        mkdir -p "$_podman_conf_dir"
        # Standard minimal "trust everything" policy -- the same one
        # containers/skopeo ships as its own default-policy.json
        # (containers-policy.json(5)). There's no env var to redirect
        # podman's policy.json lookup (unlike containers.conf/
        # storage.conf/registries.conf below), so this has to be a real
        # file at podman's fixed per-user lookup path -- only written when
        # neither that nor the system one already exists; a real podman/
        # containers-common install (devc.sh already requires one) ships
        # its own, more deliberate default instead.
        cat > "$_podman_conf_dir/policy.json" <<'POLICY'
{
    "default": [
        { "type": "insecureAcceptAnything" }
    ],
    "transports": {
        "docker-daemon": {
            "": [{ "type": "insecureAcceptAnything" }]
        }
    }
}
POLICY
      fi
      if [[ ! -s /etc/containers/registries.conf && ! -s "$_podman_conf_dir/registries.conf" ]]; then
        mkdir -p "$_podman_conf_dir"
        echo 'unqualified-search-registries = ["docker.io"]' > "$_podman_conf_dir/registries.conf"
      fi

      if [[ ! -S "$_podman_sock" ]] || ! podman --url "unix://$_podman_sock" info >/dev/null 2>&1; then
        mkdir -p "$(dirname "$_podman_sock")"
        nohup podman system service --time=0 "unix://$_podman_sock" >/dev/null 2>&1 &
        disown
        for _i in $(seq 1 40); do
          [[ -S "$_podman_sock" ]] && break
          sleep 0.25
        done
      fi

      if [[ -S "$_podman_sock" ]]; then
        export DOCKER_HOST="unix://$_podman_sock"
        export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$_podman_sock"
      else
        echo "podman: API socket did not come up at $_podman_sock" >&2
      fi
      unset _podman_conf_dir
    fi
    unset _podman_sock _i
  '';
}
