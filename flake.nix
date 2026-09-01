
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
# gradle-wrapper.properties pins into the Nix store, pre-seeds
# `./gradlew`'s cache with it on shell entry, and isolates toolchain
# resolution to just the JDK(s) this flake provides -- see
# nix/gradle-wrapper.nix for all of that.
#
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

        gradleWrapper = import ./nix/gradle-wrapper.nix {
          inherit pkgs;
          wrapperPropertiesFile = ./gradle/wrapper/gradle-wrapper.properties;
        };

        common = with pkgs; [
          bootstrapJdk
          git
          # gradlew fetches Gradle itself; these are for everyday use.
          jq
          curl
        ] ++ gradleWrapper.extraBuildInputs;

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

        gradleShellHook = gradleWrapper.isolatedHomeHook + gradleWrapper.warmupHook;
      in
      {
        packages = {
          # Exposed mainly so `nix build .#gradleDistribution` can be used
          # to sanity-check the vendored distribution on its own.
          gradleDistribution = gradleWrapper.distExtracted;

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
            shellHook = gradleShellHook + ''
              echo "deephaven-core dev shell (bootstrap JDK $(java -version 2>&1 | head -1))"
              echo "Run: ./gradlew server-jetty-app:run"
            '';
          };

          web = pkgs.mkShell {
            buildInputs = common ++ webTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleShellHook;
          };

          python = pkgs.mkShell {
            buildInputs = common ++ pythonTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleShellHook;
          };

          cpp = pkgs.mkShell {
            buildInputs = common ++ cppTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleShellHook;
          };

          full = pkgs.mkShell {
            buildInputs = common ++ webTools ++ pythonTools ++ cppTools;
            JAVA_HOME = bootstrapJdk.home;
            shellHook = gradleShellHook;
          };
        };
      });
}
