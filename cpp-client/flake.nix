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

      mkClient = pkgs: { pnameSuffix ? "", extraCmakeFlags ? [ ] }:
        pkgs.stdenv.mkDerivation {
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
          propagatedBuildInputs = with pkgs; [
            arrow-cpp grpc protobuf immer
            abseil-cpp c-ares openssl re2 zlib
          ];

          cmakeFlags = [
            "-DBUILD_SHARED_LIBS=ON"
            "-DDH_PROTO_DIR=${dh-proto.protoSrc}"
          ] ++ extraCmakeFlags;

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
