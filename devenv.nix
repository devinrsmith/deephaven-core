# Reproducible development environment for deephaven-core, via devenv.sh
# (https://devenv.sh).
#
# This is deliberately minimal: just the bootstrap JDK needed to run
# `./gradlew`. The Python, Node, and protoc parts of the build run inside
# Docker images (see the docker-* subprojects), so they need a running
# Docker or Podman rather than host toolchains.
#
# Usage:
#   devenv shell
#
# Every shell entry also vendors the exact Gradle distribution
# gradle-wrapper.properties pins into the Nix store, pre-seeds
# `./gradlew`'s cache with it, and isolates toolchain resolution to just
# the JDK this file provides -- see the nix-gradle-wrapper input
# (github:devinrsmith/nix-gradle-wrapper, declared in devenv.yaml) for all
# of that. devenv has no built-in equivalent for either (confirmed against
# its current source/docs).
{ pkgs, inputs, ... }:
let
  # Gradle 9.x (this repo's wrapper version, see
  # gradle/wrapper/gradle-wrapper.properties) requires Java 17+ just to
  # launch.
  bootstrapJdk = pkgs.temurin-bin-21;

  # bootstrapJdk's real, toolchain-detectable home -- see the comment above
  # env.JAVA_HOME below for why Darwin needs the nested bundle path.
  bootstrapJdkHome =
    if bootstrapJdk ? bundle
    then "${bootstrapJdk.bundle}/Contents/Home"
    else bootstrapJdk.home;

  gradleWrapper = import "${inputs.nix-gradle-wrapper}/gradle-wrapper.nix" {
    inherit pkgs;
    wrapperPropertiesFile = ./gradle/wrapper/gradle-wrapper.properties;
    # Namespaces the isolated GRADLE_USER_HOME
    # ($XDG_CACHE_HOME/deephaven-core-nix-gradle-home).
    name = "deephaven-core";
    # Worst-case per-worker heap for org.gradle.workers.max sizing:
    # engine/table/build.gradle's test maxHeapSize (3500m), the largest in
    # the build -- keep in sync if that ever changes.
    perWorkerMemBytes = 3500 * 1024 * 1024;
    # Pin the JVM that runs the Gradle daemon (org.gradle.java.home) to the
    # same JDK as JAVA_HOME, rather than whatever JAVA_HOME/PATH happen to
    # resolve to when ./gradlew starts.
    javaHome = bootstrapJdkHome;
  };

  # Finds a Docker-API engine for the Docker-API-consuming Gradle tasks
  # (Testcontainers, the bmuschko gradle-docker-plugin), without a
  # per-machine DOCKER_HOST hardcoded anywhere. Never starts or configures
  # an engine; it only wires up one that is already running.
  #
  # 1. An already-set DOCKER_HOST is an explicit choice and is never
  #    overridden (a warning is printed if nothing answers on it).
  # 2. A working Docker engine on the default socket is left alone -- the
  #    Java Docker clients already find /var/run/docker.sock by themselves.
  # 3. Otherwise look for Podman's API socket: first wherever `podman info`
  #    reports it, then -- if podman isn't installed, `podman info` fails,
  #    or nothing answers there -- the known locations: $CONTAINER_HOST,
  #    rootless ($XDG_RUNTIME_DIR/podman/podman.sock), rootful
  #    (/run/podman/podman.sock). The rootless path embeds your UID, so a
  #    value that works on one contributor's machine won't on another's.
  #    The first that answers becomes DOCKER_HOST, plus
  #    TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE, which Testcontainers needs
  #    with Podman.
  #
  # "Answers" means a Docker-API GET /_ping returns OK (Podman's
  # compatibility API serves it too), not just that a socket file exists --
  # a stale socket from a stopped service doesn't count. The shell carries
  # no Docker CLI, so nixpkgs' curl does the ping, referenced by store path
  # so it isn't added to PATH. The chosen engine is printed on entry, and a
  # warning if none is reachable.
  dockerHostHook = ''
    _dh_ping() { # unix socket path -> success if a Docker-API engine answers
      [[ -S "$1" ]] && [[ "$(${pkgs.curl}/bin/curl -fsS --max-time 3 --unix-socket "$1" http://localhost/_ping 2>/dev/null)" == OK ]]
    }
    _dh_engine=""
    if [[ -n "''${DOCKER_HOST:-}" ]]; then
      if [[ "$DOCKER_HOST" != unix://* ]]; then
        _dh_engine="$DOCKER_HOST (preset, not checked)"
      elif _dh_ping "''${DOCKER_HOST#unix://}"; then
        _dh_engine="$DOCKER_HOST (preset)"
      else
        echo "warning: DOCKER_HOST=$DOCKER_HOST is set but no Docker-API engine answers there." >&2
      fi
    elif _dh_ping /var/run/docker.sock; then
      _dh_engine="Docker (/var/run/docker.sock)"
    else
      _dh_candidates=()
      if command -v podman >/dev/null 2>&1 \
          && _dh_sock="$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null)"; then
        _dh_candidates+=("$_dh_sock")
      fi
      _dh_candidates+=(
        "''${CONTAINER_HOST:-}"
        "''${XDG_RUNTIME_DIR:+$XDG_RUNTIME_DIR/podman/podman.sock}"
        /run/podman/podman.sock
      )
      for _dh_sock in "''${_dh_candidates[@]}"; do
        # Depending on podman version, a reported path may or may not
        # carry a "unix://" prefix; CONTAINER_HOST always does.
        _dh_sock="''${_dh_sock#unix://}"
        if [[ -n "$_dh_sock" ]] && _dh_ping "$_dh_sock"; then
          export DOCKER_HOST="unix://$_dh_sock"
          export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$_dh_sock"
          _dh_engine="Podman ($DOCKER_HOST)"
          break
        fi
      done
    fi
    if [[ -n "$_dh_engine" ]]; then
      echo "engine: $_dh_engine"
    elif [[ -z "''${DOCKER_HOST:-}" ]]; then
      echo "warning: no Docker or Podman engine reachable; Docker-based tasks will fail until one is running." >&2
      echo "         Start Docker, or Podman's API socket (e.g. 'systemctl --user start podman.socket')," >&2
      echo "         or point DOCKER_HOST at one, then re-enter the shell." >&2
    fi
    unset -f _dh_ping
    unset _dh_engine _dh_candidates _dh_sock
  '';

  # Native libraries that dependencies unpack from their jars and load at
  # runtime (e.g. brotli-codec's libbrotli.so, used by
  # :extensions-parquet-table:brotliTest) expect libstdc++.so.6 from the
  # system's default library path. NixOS has none, so point the loader at
  # nixpkgs' copy there. Only on NixOS: elsewhere the distro's own
  # libstdc++ is already found, and putting nixpkgs' (which may need a
  # newer glibc than the host's) on LD_LIBRARY_PATH could break host
  # programs run from this shell.
  nixosLibstdcxxHook = pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isLinux ''
    if [[ -e /etc/NIXOS ]]; then
      export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    fi
  '';
in
{
  languages.java = {
    enable = true;
    jdk.package = bootstrapJdk; # also sets JAVA_HOME (overridden below on
    # Darwin -- see env.JAVA_HOME)
    # Not using languages.java.gradle -- we run the repo's own ./gradlew,
    # and a second Nix-provided `gradle` binary on PATH bound to the same
    # JDK would just be a confusing, unused alternative sitting alongside
    # it.
  };

  # devenv's languages.java module sets JAVA_HOME to bootstrapJdk.home
  # unconditionally (cachix/devenv's languages/java.nix). On Linux that IS
  # the real JDK home (temurin-bin's Linux layout is flat), matching what a
  # running JVM reports as its own `java.home` system property. On Darwin,
  # though, nixpkgs' temurin-bin output is a symlink farm: the real JDK
  # content lives nested at
  # $out/Library/Java/JavaVirtualMachines/<name>-<major>.jdk/Contents/Home,
  # and $out itself (== .home == what devenv sets JAVA_HOME to) is just
  # top-level symlinks into that directory (confirmed against
  # pkgs/development/compilers/temurin-bin/jdk-darwin-base.nix). A JVM
  # launched through those symlinks reports its own `java.home` as the
  # *resolved* nested path, not $out -- so Gradle's toolchain detection
  # sees two different Location strings for the exact same JDK ("Detected
  # by: environment variable 'JAVA_HOME'" at $out, "Detected by: Current
  # JVM" at the nested Contents/Home) and lists it twice.
  #
  # nixpkgs' Darwin JDK builder already computes that nested bundle
  # directory itself and exposes it via a `bundle` passthru (added by
  # nixpkgs#375212, "treewide: standardize JDKs on darwin") -- appending
  # Contents/Home to that gives the exact canonical path a running JVM
  # will report, without us hand-guessing the vendor/version-specific
  # "<name>-<major>.jdk" bundle name. Only present on Darwin (the Linux
  # builder has no `bundle` attribute at all), so its presence is what to
  # branch on. mkForce is needed because languages.java already sets this
  # option (a plain conflicting assignment would otherwise error).
  env.JAVA_HOME = pkgs.lib.mkForce bootstrapJdkHome;

  packages = gradleWrapper.extraBuildInputs;

  enterShell = gradleWrapper.isolatedHomeHook + gradleWrapper.warmupHook + ''
    echo "deephaven-core dev shell (bootstrap JDK $(java -version 2>&1 | head -1))"
    echo "Run: ./gradlew server-jetty-app:run"
  '' + dockerHostHook + nixosLibstdcxxHook;

  # Docker-API access (Testcontainers-based `testOutOfBand` tests in
  # extensions/kafka, extensions/iceberg/s3, etc.; the bmuschko
  # gradle-docker-plugin's :docker-* subprojects) needs a real Docker or
  # Podman install already running on your host -- this file doesn't
  # provision one, it only wires up DOCKER_HOST for whatever's already
  # there (see dockerHostHook above). devenv's own containers.*
  # option builds OCI images from this environment; it isn't a
  # Docker-API-compatible daemon/socket, so it wouldn't help here anyway.
}
