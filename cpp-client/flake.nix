{
  description = "Deephaven C++ client (dhcore + dhclient)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Relative path input: resolved inside this repository and not pinned by hash
    # in flake.lock, so edits to the .proto files are picked up without relocking.
    dh-proto.url = "path:../proto/proto-backplane-grpc";
  };

  outputs = { self, nixpkgs, dh-proto }:
    let
      inherit (nixpkgs) lib;
      # Windows is built with vcpkg (README-windows.md); macOS is untested.
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      version = builtins.head (builtins.match ".*set\\(deephaven_VERSION ([0-9.]+)\\).*"
        (builtins.readFile ./deephaven/CMakeLists.txt));

      # Arrow with only what the client uses (core, IPC, Flight, CSV, and the LZ4,
      # ZSTD and zlib codecs that build-dependencies.sh also enables), for the
      # static build. nixpkgs' arrow-cpp enables nearly every component, and its
      # static variant is not in the binary cache, so this is what gets built.
      mkMinimalArrow = spkgs:
        (spkgs.arrow-cpp.override {
          enableFlight = true; # off by default for pkgsStatic, which counts as cross
          enableJemalloc = false;
          enableS3 = false;
          enableGcs = false;
          enableAzure = false;
        }).overrideAttrs (old: {
          pname = "arrow-cpp-minimal";
          buildInputs = with spkgs; [
            abseil-cpp c-ares grpc lz4 openssl protobuf re2 zlib zstd
          ];
          # pkgsStatic's stdenv propagates the original buildInputs (static
          # libraries' dependencies must reach the final link); keep that to the
          # minimal set as well.
          propagatedBuildInputs = with spkgs; [
            abseil-cpp c-ares grpc lz4 openssl protobuf re2 zlib zstd
          ];
          cmakeFlags = [
            (lib.cmakeBool "CMAKE_FIND_PACKAGE_PREFER_CONFIG" true)
            (lib.cmakeBool "ARROW_BUILD_SHARED" false)
            (lib.cmakeBool "ARROW_BUILD_STATIC" true)
            (lib.cmakeBool "ARROW_BUILD_TESTS" false)
            (lib.cmakeBool "ARROW_BUILD_UTILITIES" false)
            (lib.cmakeFeature "ARROW_DEPENDENCY_SOURCE" "SYSTEM")
            (lib.cmakeBool "ARROW_DEPENDENCY_USE_SHARED" false)
            (lib.cmakeBool "ARROW_IPC" true)
            (lib.cmakeBool "ARROW_FLIGHT" true)
            # gRPC's CMake config omits its plugin targets when cross-compiling.
            (lib.cmakeFeature "ARROW_GRPC_CPP_PLUGIN"
              (lib.getExe' spkgs.buildPackages.grpc "grpc_cpp_plugin"))
            (lib.cmakeBool "ARROW_CSV" true)
            (lib.cmakeBool "ARROW_WITH_LZ4" true)
            (lib.cmakeBool "ARROW_WITH_ZSTD" true)
            (lib.cmakeBool "ARROW_WITH_ZLIB" true)
            (lib.cmakeBool "ARROW_JEMALLOC" false)
            (lib.cmakeBool "ARROW_MIMALLOC" false)
            (lib.cmakeBool "ARROW_USE_GLOG" false)
            (lib.cmakeBool "ARROW_WITH_BACKTRACE" false)
            (lib.cmakeFeature "ARROW_SIMD_LEVEL" "NONE")
            (lib.cmakeFeature "ARROW_RUNTIME_SIMD_LEVEL" "NONE")
          ];
          env = removeAttrs old.env [
            "ARROW_TEST_DATA" "PARQUET_TEST_DATA" "GTEST_FILTER" "ARROW_SUBSTRAIT_URL" "SNAPPY_HOME"
          ];
          doInstallCheck = false;
        });

      mkClient = pkgs: { pnameSuffix ? "", extraCmakeFlags ? [ ], static ? false }:
        let
          deps = if static then pkgs.pkgsStatic else pkgs;
          arrow = if static then mkMinimalArrow deps else deps.arrow-cpp;
        in
        deps.stdenv.mkDerivation {
          pname = "deephaven-cpp-client${pnameSuffix}";
          inherit version;

          src = lib.fileset.toSource {
            root = ./deephaven;
            fileset = ./deephaven;
          };

          nativeBuildInputs = with pkgs; [ cmake ninja ];
          # Propagated because the installed deephavenConfig.cmake does
          # find_dependency() on these. The second line is what nixpkgs' gRPC
          # CMake config itself needs to resolve its imported targets.
          #
          # Possible later work: the runtime closure of a program linking the
          # client is ~455 MiB. Splitting outputs (out/dev) would keep this
          # propagation, and with it the -dev outputs, out of the runtime
          # closure. Overriding arrow-cpp with enableS3/enableGcs = false would
          # drop google-cloud-cpp (~290M) and probably most of Boost, but Arrow
          # would then be built from source rather than fetched from the cache.
          propagatedBuildInputs = [ arrow ] ++ (with deps; [
            grpc protobuf immer
            abseil-cpp c-ares openssl re2 zlib
          ]);

          cmakeFlags = [
            "-DBUILD_SHARED_LIBS=${if static then "OFF" else "ON"}"
            "-DDH_STATIC_CLIENT=${if static then "ON" else "OFF"}"
            "-DDH_PROTO_DIR=${dh-proto.protoSrc}"
          ] ++ lib.optionals static [
            # Run the build machine's code generators (same versions as the static
            # libraries); see DH_GRPC_CPP_PLUGIN in dhclient/CMakeLists.txt.
            "-DDH_PROTOC=${lib.getExe' deps.buildPackages.protobuf "protoc"}"
            "-DDH_GRPC_CPP_PLUGIN=${lib.getExe' deps.buildPackages.grpc "grpc_cpp_plugin"}"
          ] ++ extraCmakeFlags;

          # Ship the static executables fully stripped (Nix strips only debug info
          # by default). --gc-sections saves little (~0.6 MB) because the static
          # dependencies are not built with -ffunction-sections, but costs nothing.
          stripAllList = lib.optionals static [ "bin" ];
          env.NIX_LDFLAGS = lib.optionalString static "--gc-sections";

          # The examples are not installed by CMake; in the static build, ship
          # hello_world as the standalone executable.
          postInstall = lib.optionalString static ''
            install -Dm755 examples/hello_world/hello_world $out/bin/hello_world
          '';

          # dhclient_tests needs a running Deephaven server, so it is installed
          # (as with the Docker build) but not run here.
          doCheck = false;

          meta = {
            description = "Deephaven C++ client libraries (dhcore, dhclient)";
            homepage = "https://deephaven.io/core/docs/";
            license = lib.licenses.asl20;
            platforms = systems;
          };
        };
    in
    {
      packages = forAllSystems (pkgs: rec {
        deephaven-cpp-client = mkClient pkgs { };
        default = deephaven-cpp-client;
        # Static libraries plus fully static (musl) hello_world and dhclient_tests
        # executables that run without Nix. Builds Arrow, gRPC, protobuf and
        # abseil from source the first time; not part of `nix flake check`.
        deephaven-cpp-client-static = mkClient pkgs {
          pnameSuffix = "-static";
          static = true;
        };
      });

      checks = forAllSystems (pkgs: {
        gcc = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
        asan = mkClient pkgs {
          pnameSuffix = "-asan";
          extraCmakeFlags = [ "-DSANITIZE_ADDRESS=ON" ];
        };
        # Builds hello_world out of tree against the installed package, the way
        # a downstream project would, to exercise deephavenConfig.cmake.
        consumer = pkgs.stdenv.mkDerivation {
          name = "deephaven-cpp-client-consumer-check";
          src = lib.fileset.toSource {
            root = ./deephaven/examples/hello_world;
            fileset = ./deephaven/examples/hello_world/main.cc;
          };
          nativeBuildInputs = with pkgs; [ cmake ninja ];
          buildInputs = [ self.packages.${pkgs.stdenv.hostPlatform.system}.default ];
          postUnpack = ''
            cat > $sourceRoot/CMakeLists.txt <<'EOF'
            cmake_minimum_required(VERSION 3.16)
            project(hello_world_consumer CXX)
            set(CMAKE_CXX_STANDARD 17)
            find_package(deephaven REQUIRED)
            add_executable(hello_world main.cc)
            target_link_libraries(hello_world deephaven::dhclient)
            EOF
          '';
          installPhase = "install -Dm755 hello_world $out/bin/hello_world";
        };
      });

      # `nix develop` from cpp-client/, then the usual
      #   cmake -S deephaven -B build && cmake --build build
      # The default DH_PROTO_DIR (the in-repo proto tree) is used there.
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.default ];
          packages = with pkgs; [ clang-tools gdb ];
        };
      });
    };
}
