{
  description = "Deephaven C++ client (dhcore + dhclient)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Relative path input: resolved inside this repository and not pinned by hash
    # in flake.lock, so edits to the .proto files are picked up without relocking.
    dh-proto.url = "path:../proto/proto-backplane-grpc";
    # Sources of the pydeephaven-ticking wheel (Cython over dhcore). Also a relative
    # path input; narrowed with builtins.path below so the wheels depend only on it.
    py-client-ticking = {
      url = "path:../py/client-ticking";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, dh-proto, py-client-ticking }:
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

      # --- pydeephaven-ticking wheels, built with Zig instead of in a manylinux container ---
      #
      # zig cc/c++ links against stub libraries for a chosen glibc version and statically
      # links its own libc++, so the extension needs nothing from the host but glibc 2.17.
      # auditwheel then tags (and checks) the wheel as manylinux2014.

      # The version Gradle passes as DEEPHAVEN_VERSION: <major>.<minor>[-<qualifier>].
      deephavenVersion =
        let
          props = builtins.readFile ../gradle.properties;
          prop = name: builtins.head (builtins.match ".*\n${name}=([^\n]*)\n.*" props);
          qualifier = prop "deephavenBaseQualifier";
        in
        "${prop "deephavenMajorVersion"}.${prop "deephavenMinorVersion"}"
        + lib.optionalString (qualifier != "") "-${qualifier}";

      wheelCpu = pkgs: pkgs.stdenv.hostPlatform.parsed.cpu.name;
      wheelPlatform = pkgs: "manylinux2014_${wheelCpu pkgs}";

      # The official Zig release (statically linked, with its own LLVM) rather than nixpkgs'
      # zig: the nixpkgs zig 0.16.0 at the pinned revision emits objects whose undefined
      # symbols reach the linker with empty names ("undefined symbol: "), so the extension
      # fails to link, while this build of the same version links it fine.
      zigVersion = "0.16.0";
      mkZigBin = pkgs:
        let
          cpu = wheelCpu pkgs;
          hashes = {
            x86_64 = "sha256-cOSWZKdDdLSLUebz/fv0N/Y5XUJQkFBYi9SavlK6PQA=";
            aarch64 = "sha256-6ksJv7IuxvbGzqxXq2PvtrRuF6sI0h9p86SLOOFTTxc=";
          };
        in
        pkgs.stdenvNoCC.mkDerivation {
          pname = "zig-bin";
          version = zigVersion;
          src = pkgs.fetchurl {
            url = "https://ziglang.org/download/${zigVersion}/zig-${cpu}-linux-${zigVersion}.tar.xz";
            hash = hashes.${cpu};
          };
          installPhase = ''
            mkdir -p $out/bin
            cp -r . $out/lib
            ln -s $out/lib/zig $out/bin/zig
          '';
          dontFixup = true;
        };

      mkZigToolchain = pkgs:
        let
          zig = mkZigBin pkgs;
          target = "${wheelCpu pkgs}-linux-gnu.2.17";
          # Zig's default cache is under $HOME, which is not writable in the Nix sandbox.
          tool = name: args: pkgs.writeShellScriptBin name ''
            export ZIG_GLOBAL_CACHE_DIR="''${ZIG_GLOBAL_CACHE_DIR:-''${TMPDIR:-/tmp}/zig-cache}"
            export ZIG_LOCAL_CACHE_DIR="''${ZIG_LOCAL_CACHE_DIR:-$ZIG_GLOBAL_CACHE_DIR}"
            exec ${zig}/bin/zig ${args} "$@"
          '';
        in
        pkgs.symlinkJoin {
          name = "zig-toolchain-${target}";
          paths = [
            (tool "zig-cc" "cc -target ${target}")
            (tool "zig-c++" "c++ -target ${target}")
            (tool "zig-ar" "ar")
            (tool "zig-ranlib" "ranlib")
          ];
        };

      # dhcore_static (its only dependency, immer, is header-only) built with the Zig toolchain.
      mkDhcoreZig = pkgs:
        let zigTools = mkZigToolchain pkgs; in
        pkgs.stdenvNoCC.mkDerivation {
          pname = "dhcore-zig-${wheelCpu pkgs}-glibc2.17";
          inherit version;
          src = lib.fileset.toSource {
            root = ./deephaven;
            fileset = ./deephaven;
          };
          nativeBuildInputs = [ pkgs.cmake pkgs.ninja zigTools ];
          buildInputs = [ pkgs.immer ];
          cmakeFlags = [
            "-DDHCORE_ONLY=ON"
            "-DDH_STATIC_CLIENT=ON" # only dhcore_static
            "-DCMAKE_C_COMPILER=zig-cc"
            "-DCMAKE_CXX_COMPILER=zig-c++"
            "-DCMAKE_AR=${zigTools}/bin/zig-ar"
            "-DCMAKE_RANLIB=${zigTools}/bin/zig-ranlib"
          ];
          # With DHCORE_ONLY, CMake installs only the headers.
          postInstall = ''
            install -Dm644 dhcore/libdhcore_static.a $out/lib/libdhcore_static.a
          '';
          dontStrip = true;
        };

      # One manylinux2014 wheel for one Python version.
      mkTickingWheel = pkgs: python:
        let
          zigTools = mkZigToolchain pkgs;
          dhcore = mkDhcoreZig pkgs;
          pythonEnv = python.withPackages (ps: [ ps.cython ps.packaging ps.setuptools ps.wheel ]);
          # Export only the module's init function, keeping the bundled libc++ private.
          exportMap = pkgs.writeText "pydeephaven-ticking.map" "{ global: PyInit__core; local: *; };";
          pyTag = "cp${lib.replaceStrings [ "." ] [ "" ] python.pythonVersion}";
        in
        pkgs.stdenvNoCC.mkDerivation {
          pname = "pydeephaven-ticking-wheel-${pyTag}";
          version = deephavenVersion;
          src = builtins.path {
            name = "py-client-ticking";
            path = py-client-ticking;
          };
          # auditwheel is run by path: as a Python 3.14 application in nativeBuildInputs,
          # its setup hook would leak that Python into this build.
          nativeBuildInputs = [ pythonEnv zigTools pkgs.patchelf ];
          env = {
            DEEPHAVEN_VERSION = deephavenVersion;
            CC = "zig-cc";
            CXX = "zig-c++";
            LDSHARED = "zig-c++ -shared";
            LDCXXSHARED = "zig-c++ -shared";
            CPPFLAGS = "-I${dhcore}/include";
            LDFLAGS = "-L${dhcore}/lib -s -Wl,--version-script=${exportMap}";
          };
          buildPhase = ''
            runHook preBuild
            python setup.py bdist_wheel
            runHook postBuild
          '';
          # auditwheel fails here if the wheel does not meet the manylinux2014 policy.
          installPhase = ''
            runHook preInstall
            ${lib.getExe pkgs.auditwheel} repair --plat ${wheelPlatform pkgs} --wheel-dir $out dist/*.whl
            runHook postInstall
          '';
          dontFixup = true;
        };

      # Every CPython version nixpkgs provides headers for (it no longer carries 3.9/3.10).
      wheelPythons = pkgs: lib.filter (p: p != null) (map
        (n: let p = builtins.tryEval (pkgs.${n} or null); in
          if p.success && p.value != null && (builtins.tryEval p.value.version).success
          then p.value else null)
        [ "python39" "python310" "python311" "python312" "python313" "python314" ]);

      tickingWheels = pkgs: lib.listToAttrs (map
        (python: let w = mkTickingWheel pkgs python; in lib.nameValuePair w.pname w)
        (wheelPythons pkgs));
    in
    {
      packages = forAllSystems (pkgs: (tickingWheels pkgs) // rec {
        # All pydeephaven-ticking wheels in one directory (result/*.whl).
        pydeephaven-ticking-wheels = pkgs.symlinkJoin {
          name = "pydeephaven-ticking-wheels-${deephavenVersion}";
          paths = lib.attrValues (tickingWheels pkgs);
        };
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
        # Building it runs auditwheel's manylinux2014 policy check.
        ticking-wheel = mkTickingWheel pkgs (lib.last (wheelPythons pkgs));
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
