
# Reproducible development environment for deephaven-core.
#
# This is deliberately narrow: it provisions the tools a human needs on
# PATH to run `./gradlew`, work on the web client, or build the C++ client
# -- it does not try to replace Gradle's own JDK toolchain provisioning
# (org.gradle.toolchains.foojay-resolver-convention, declared in
# settings.gradle), which already downloads whatever per-subproject JDK
# (11-25) a given build target requests. Pinning every one of those in Nix
# too would just be a second, competing source of truth for the same
# versions -- so `default` only pins the *bootstrap* JDK needed to launch
# Gradle itself, exactly like .devcontainer/project.Dockerfile does today.
#
# Usage:
#   nix develop            # core Java/Gradle shell (bootstrap JDK + git)
#   nix develop .#web      # + Node, for web/ frontend work
#   nix develop .#python   # + Python + native build toolchain, for py-server/jpy
#   nix develop .#cpp      # + cmake/g++/etc., for cpp-client
#   nix develop .#full     # everything above, one shell
#
# Every shell above also vendors the exact Gradle distribution
# gradle-wrapper.properties pins into the Nix store and pre-seeds
# `./gradlew`'s cache with it on shell entry (see gradleWarmupHook below),
# so a first `./gradlew` run doesn't need network access for that download.
# direnv users: `echo "use flake" > .envrc && direnv allow` picks up
# `default` automatically; use `use flake .#full` for the combined shell.
{
  description = "deephaven-core development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Gradle 9.7.1 (this repo's wrapper version, see
        # gradle/wrapper/gradle-wrapper.properties) requires Java 17+ just
        # to launch. 21 is what .devcontainer/project.Dockerfile installs
        # today -- keep them in sync if that ever changes.
        bootstrapJdk = pkgs.temurin-bin-21;

        common = with pkgs; [
          bootstrapJdk
          git
          # gradlew fetches Gradle itself; these are for everyday use.
          jq
          curl
          # Only for gradleWarmupHook's base36 conversion below.
          bc
        ];

        webTools = with pkgs; [
          # Track web/client-api/types/.nvmrc.
          nodejs_24
        ];

        pythonTools = with pkgs; [
          # Matches python-version in .github/workflows/quick-ci.yml.
          # jpy (py-server's JNI bridge) compiles a native extension
          # against this interpreter, so a compiler toolchain is required.
          python312
          gcc
          gnumake
        ];

        cppTools = with pkgs; [
          # Mirrors cpp-client/README.md's `apt install` line.
          cmake
          gcc
          zlib
          bzip2
          openssl
          pkg-config
        ];

        # ---- Vendor the Gradle wrapper's distribution ----------------------
        #
        # `./gradlew` downloads its own Gradle distribution on first run --
        # normally a good thing (see the bootstrap-JDK-only comment above),
        # but it means a fresh `nix develop` still needs network access for
        # that one download. Since the exact version and checksum are
        # already pinned in gradle-wrapper.properties, we can fetch and
        # unpack that same file as a Nix derivation (reusing its checksum,
        # not a new trust decision) and pre-seed the wrapper's on-disk
        # cache, so `./gradlew` finds it already there and skips the
        # download. gradle-wrapper.properties stays the single source of
        # truth -- read here, never duplicated -- so a version bump there
        # just changes what gets fetched, with nothing to keep in sync by
        # hand.
        gradleWrapperProps = pkgs.lib.splitString "\n"
          (builtins.readFile ./gradle/wrapper/gradle-wrapper.properties);
        gradleWrapperProp = key:
          let
            prefix = key + "=";
            matches = builtins.filter (pkgs.lib.hasPrefix prefix) gradleWrapperProps;
          in
          pkgs.lib.removePrefix prefix (builtins.head matches);

        # Java .properties escapes ":" as "\:" -- unescape it back to a URL.
        gradleDistUrl =
          builtins.replaceStrings [ "\\:" ] [ ":" ] (gradleWrapperProp "distributionUrl");
        gradleDistSha256 = gradleWrapperProp "distributionSha256Sum";

        # ".../gradle-9.7.1-all.zip" -> zipBase "gradle-9.7.1-all", dirName "gradle-9.7.1"
        gradleZipBase =
          pkgs.lib.removeSuffix ".zip" (pkgs.lib.last (pkgs.lib.splitString "/" gradleDistUrl));
        gradleDirName =
          pkgs.lib.removeSuffix "-bin" (pkgs.lib.removeSuffix "-all" gradleZipBase);

        gradleDistZip = pkgs.fetchurl {
          url = gradleDistUrl;
          sha256 = gradleDistSha256;
        };

        # The wrapper's on-disk layout is $GRADLE_USER_HOME/wrapper/dists/
        # <zipBase>/<hash>/<dirName>, where <hash> is
        # base36(md5(distributionUrl)) -- an internal, undocumented detail
        # of Gradle's wrapper (org.gradle.wrapper.PathAssembler), confirmed
        # empirically against a real `./gradlew` run rather than assumed.
        # If a future Gradle wrapper version changes that scheme, this
        # degrades gracefully: the pre-seeded cache dir just won't be found,
        # and `./gradlew` falls back to its normal download.
        gradleDistExtracted = pkgs.runCommand "gradle-dist-${gradleDirName}"
          { nativeBuildInputs = [ pkgs.unzip ]; }
          ''
            unzip -q ${gradleDistZip} -d "$TMPDIR/unpacked"
            mv "$TMPDIR/unpacked/${gradleDirName}" "$out"
          '';

        # Only the base36-of-MD5 conversion happens at shell-hook runtime
        # (via bc -- Nix's own integers are too narrow for a 128-bit hash);
        # the MD5 itself is computed at eval time with Nix's builtin hasher.
        gradleDistMd5Hex = builtins.hashString "md5" gradleDistUrl;

        gradleWarmupHook = ''
          _gradle_home="''${GRADLE_USER_HOME:-$HOME/.gradle}"
          _gradle_hash_hex=$(printf '%s' "${gradleDistMd5Hex}" | tr 'a-f' 'A-F')
          _gradle_hash_digits=$(BC_LINE_LENGTH=0 bc <<< "obase=36; ibase=16; $_gradle_hash_hex")
          _gradle_hash_dir=""
          _gradle_b36chars='0123456789abcdefghijklmnopqrstuvwxyz'
          for _d in $_gradle_hash_digits; do
            _gradle_hash_dir="''${_gradle_hash_dir}''${_gradle_b36chars:$((10#$_d)):1}"
          done
          _gradle_dist_dir="$_gradle_home/wrapper/dists/${gradleZipBase}/$_gradle_hash_dir"
          if [[ ! -e "$_gradle_dist_dir/${gradleZipBase}.zip.ok" ]]; then
            mkdir -p "$_gradle_dist_dir"
            ln -sfn "${gradleDistExtracted}" "$_gradle_dist_dir/${gradleDirName}"
            touch "$_gradle_dist_dir/${gradleZipBase}.zip.ok"
          fi
          unset _gradle_home _gradle_hash_hex _gradle_hash_digits _gradle_hash_dir _gradle_b36chars _gradle_dist_dir _d
        '';
      in
      {
        packages = {
          # Exposed mainly so `nix build .#gradleDistribution` can be used
          # to sanity-check the vendored distribution on its own.
          gradleDistribution = gradleDistExtracted;

          # A single, non-content-hashed store path bundling every tool the
          # `full` devShell provides. Consumed by
          # .devcontainer/project.Dockerfile via `nix build .#devEnv
          # --out-link <stable-path>` so it can be referenced by a fixed
          # Dockerfile `ENV PATH=...` -- unlike a raw /nix/store/<hash>-...
          # path, the --out-link target doesn't change between builds even
          # as the store path it resolves to does.
          devEnv = pkgs.buildEnv {
            name = "deephaven-dev-env";
            paths = common ++ webTools ++ pythonTools ++ cppTools;
          };
        };

        devShells = {
          default = pkgs.mkShell {
            buildInputs = common;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleWarmupHook + ''
              echo "deephaven-core dev shell (bootstrap JDK $(java -version 2>&1 | head -1))"
              echo "Run: ./gradlew server-jetty-app:run"
            '';
          };

          web = pkgs.mkShell {
            buildInputs = common ++ webTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleWarmupHook;
          };

          python = pkgs.mkShell {
            buildInputs = common ++ pythonTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleWarmupHook;
          };

          cpp = pkgs.mkShell {
            buildInputs = common ++ cppTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleWarmupHook;
          };

          full = pkgs.mkShell {
            buildInputs = common ++ webTools ++ pythonTools ++ cppTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleWarmupHook;
          };
        };
      });
}
