# Building the C++ client on Ubuntu 20.04 / 22.04

These instructions show how to install and run the Deephaven C++ client, its
dependencies, its unit tests on Linux. If you are looking for Windows instructions,
see the file README-windows.md.

For an index of the C++ client's documentation see [`README.md`](README.md); for how the client is
implemented see [`DESIGN.md`](DESIGN.md).

We have tested these instructions in Ubuntu 22.04 with the default
C++ compiler and tool suite (cmake etc).
We have used the instructions in the past to build
for older Ubuntu versions (20.04) and for some Fedora versions, but we don't regularly test
on them anymore so we do not guarantee they are current for those platforms.

# Before you build the client

To actually use Deephaven, for example running these examples and unit
tests, you will eventually need to have a server running. If you have
an existing server running Deephaven Core, you should be able to
connect to that. However, if you don't have one, you can follow the
instructions [here](https://deephaven.io/core/docs/getting-started/launch-build/).

Note that although the client is supported on both Linux and Windows,
at this time it's only possible to build a server on Linux. Building a
server on Windows is not currently supported.

You can build and install client libraries, tests, and examples
without having a server installed. However you will eventually need to
connect to a server when you want to run them.

# Building the C++ client with Nix

As an alternative to the Ubuntu instructions below, `cpp-client/flake.nix` describes the
whole build environment: compiler, CMake, and every dependency library, pinned by
`cpp-client/flake.lock`. It works on any x86-64 or aarch64 Linux distribution with
[Nix](https://nixos.org/download/) installed and flakes enabled, and needs neither
`build-dependencies.sh` nor `env.sh`. Prebuilt dependencies come from the public Nix binary
cache, so nothing but the client itself is compiled.

The dependency versions are whatever the pinned nixpkgs revision provides, not the versions
`build-dependencies.sh` builds; `nix flake update nixpkgs` moves them forward.

Build and install into the Nix store (the result is symlinked at `./result`, with the
libraries in `result/lib`, headers in `result/include`, the CMake package config in
`result/lib/cmake/deephaven`, and `dhclient_tests` in `result/bin`):

```
cd $DHSRC/deephaven-core/cpp-client
nix build
```

For development, enter the shell and use CMake as usual:

```
cd $DHSRC/deephaven-core/cpp-client
nix develop
cmake -S deephaven -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_SHARED_LIBS=ON
cmake --build build
```

`nix flake check` builds the client, an AddressSanitizer variant, and a small out-of-tree
program that uses the installed package through `find_package(deephaven)`. The tests are
not run by any of these, because they need a server; run `result/bin/dhclient_tests` (or
`build/tests/dhclient_tests`) against one as described in step 8 below.

Note that the libraries built this way refer to their dependencies by `/nix/store` paths,
so they are meant for use on the machine that built them (or one sharing its Nix store),
not for copying to other systems.

## Static build (standalone executables)

```
cd $DHSRC/deephaven-core/cpp-client
nix build .#deephaven-cpp-client-static
```

This builds the client and all of its dependencies statically against musl, producing
`result/bin/hello_world` and `result/bin/dhclient_tests` as fully static executables that
run on any x86-64 Linux, with no shared libraries and no dependency on the host's glibc
(tested on CentOS 7, Ubuntu 16.04, Alpine, and an empty `scratch` container image), plus
`libdhclient.a` / `libdhcore_static.a` and a CMake package for linking your own static
programs. `hello_world` is about 42 MB (11 MB compressed with `xz -9`).

The static dependencies are not in the public Nix binary cache, so the first build compiles
Arrow (a minimal configuration: core, IPC, Flight, CSV), gRPC, protobuf and abseil from
source. It uses the CMake options `DH_STATIC_CLIENT=ON`, and `DH_PROTOC` /
`DH_GRPC_CPP_PLUGIN` to run the build machine's code generators.

Not yet verified for the static build: TLS connections that rely on the system's CA
certificates (OpenSSL's built-in certificate directory points into `/nix/store`), and
performance under musl's memory allocator on data-heavy workloads.

## pydeephaven-ticking wheels (manylinux2014, no container)

```
cd $DHSRC/deephaven-core/cpp-client
nix build .#pydeephaven-ticking-wheels
```

This produces `result/*.whl`: one `manylinux2014_x86_64` (`manylinux_2_17`) wheel of
`py/client-ticking` per CPython version nixpkgs provides (currently 3.11–3.14), each about
1.2 MB. Individual versions are `.#pydeephaven-ticking-wheel-cp3XX`.

Instead of building in the manylinux2014 container, `dhcore_static` and the Cython extension
are compiled with Zig's C/C++ toolchain (`zig c++ -target x86_64-linux-gnu.2.17`), which links
against stub libraries for glibc 2.17 and statically links its own libc++. The extension
exports only `PyInit__core`, and `auditwheel repair --plat manylinux2014_x86_64` runs as part
of the build, so a wheel that does not meet the policy fails to build. The flake uses the
official Zig release from ziglang.org rather than nixpkgs' `zig`, whose build at the pinned
revision produces objects that fail to link. The wheel version comes from the root
`gradle.properties`, as in the Gradle build (e.g. `43.0-SNAPSHOT` becomes `43.0+snapshot`).

The wheels were tested against a server with the ticking tests on glibc 2.17 (Python 3.11–3.13,
pyarrow 20.0.0) and glibc 2.28 (Python 3.14, pyarrow 26.0.0). Note that pyarrow publishes
`manylinux2014` wheels only up to 20.0.0, so users on glibc 2.17 systems need to pin pyarrow.
The aarch64 variant is defined but has not been built or tested.

# Building the C++ client on Ubuntu 22.04

1. Start with an Ubuntu 22.04 install

2. Get build tools
   ```
   sudo apt update
   sudo apt install curl git g++ cmake make build-essential zlib1g-dev libbz2-dev libssl-dev pkg-config
   ```

   See the notes at the end of this document if you need the equivalent packages for Fedora.

3. Make a new directory for the Deephaven source code and assign that directory
   to a temporary shell variable. This will make subsequent build steps easier.
   ```
   export DHSRC=$HOME/src/deephaven
   mkdir -p $DHSRC
   cd $DHSRC
   ```

4. Clone deephaven-core sources.
   ```
   cd $DHSRC
   git clone https://github.com/deephaven/deephaven-core.git
   ```

5. Build and install dependencies for the Deephaven C++ client.

   Inside the $DHSRC/deephaven-core/cpp-client directory there is a script called
   `build-dependencies.sh`. This script downloads, builds, and installs the dependent
   libraries (Protobuf, re2, gflags, absl, flatbuffers, c-ares, zlib, gRPC, and Arrow).
   Decide on a directory for the dependencies to live (eg, "$HOME/dhcpp").
   Create that directory and copy the script there. If the directory already exists
   from a previous attempt, you should ensure is clean/empty.

   ```
   export DHCPP=$HOME/dhcpp
   mkdir -p $DHCPP
   cp $DHSRC/deephaven-core/cpp-client/build-dependencies.sh $DHCPP
   ```

   The three main build types of a standard cmake build are supported,
   `Release`, `Debug` and `RelWithDebInfo`.  By default. `build-dependencies.sh`
   creates a `RelWithDebInfo` build.  To create a `Release` build, set the
   environment variable `BUILD_TYPE=Release` (see note 1)

   Edit your local copy of the script if necessary to reflect your selection
   of build tools and build target; Defaults point to Ubuntu system's g++,
   cmake, and a `RelWithDebInfo` build target for cmake.
   Comments in the script will help you identifying customization points.
   Note however that defaults are tested, variations are not;
   any deviation from defaults may require manual modification of other files later,
   when building the C++ client proper.

   Next, run the script:

   ```
   cd $DHCPP
   # Below we save the output of the script to keep a record of the commands ran during the build.
   ./build-dependencies.sh 2>&1 | tee build-dependencies.log
   ```

   The execution of `build-dependencies.sh` will create, on the
   directory it is run, subdirectories `local` and `src` and a file
   `env.sh`.  The `local` directory will contain the installation of
   each dependent library (eg, `local/grpc` would be the location of
   the grpc installation, etc).  The `src` directory is where the
   sources for the libraries will be downloaded, cloned from their
   github repositories.  You can ask `build-dependencies.sh` to remove
   the downloaded sources once the libraries have been built by
   passing to it the `--clean` flag.  The default cmake build target is `RelWithDebInfo`,
   which would make it useful to keep the downloaded sources for
   debug/development purposes.  If `Release` versions are preferred,
   an environment variable `BUILD_TYPE` set to `Release` can be used
   to override the `Debug` default, but note that some libraries change their
   name (eg, `libprotobufd.a` versus `libprotobuf.a`) when compiled for
   a release version, which may conflict with assumptions elsewhere
   and may require manual modifications in other parts of the build
   (if this is your first time building, we suggest you stick with defaults).

   The `env.sh` file contains environment variable definitions in bourne
   shell syntax (compatible with bash) that will indicate to cmake
   the location of the libraries just built, via definition of a suitable
   `CMAKE_PREFIX_PATH`.
   This file is intended to be used by saying `source env.sh` from
   a shell where you plan to build the C++ client.

6. Build and install Deephaven C++ client.  Running `build-dependencies.sh` should have
   created an `env.sh` file that we source below to set relevant environment variables for
   the build.

   ```
   source $DHCPP/env.sh
   cd $DHSRC/deephaven-core/cpp-client/deephaven/
   cmake -S . -B build \
       -DCMAKE_INSTALL_LIBDIR=lib \
       -DCMAKE_CXX_STANDARD=17 \
       -DCMAKE_INSTALL_PREFIX=${DHCPP} \
       -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DBUILD_SHARED_LIBS=ON \
     && \
   VERBOSE=1 cmake --build build --target install -- -j$NCPUS
   ```

   If you need `make` to generate detailed output of the commands it is running
   (which may be helpful for debugging the paths being used etc),
   you can set the environment variable `VERBOSE=1`.
   This is true in general of cmake-generated Makefiles.
   It is useful to keep the output of make for later reference
   in case you need to be sure of the exact compiler flags
   that were used to compile the library and its objects.
   You can tweak the last command above as

   ```
   VERBOSE=1 make -j$NCPUS install 2>&1 | tee make-install.log
   ```

7. Run one Deephaven example which uses the installed client.
   This is a smoke test that the basic functionality
   is working end to end, and the build is properly configured.
   Note this assumes Deephaven server is running (see step 2),
   and the build created on step 7 is available in the same directory.

   ```
   cd $DHSRC/deephaven-core/cpp-client/deephaven/build/examples
   cd hello_world
   ./hello_world
   ```

8. (Optional) run the unit tests
   The tests get installed, you can just run:

    ```
    $DHCPP/bin/dhclient_tests
    ```

   If you want to build the tests from sources:

    ```
    cd $DHSRC/deephaven-core/cpp-client/deephaven/build/tests
    make -j$NCPUS
    ./tests
    ```
   This assumes the build created on step 7 is available in the same directory.

9. Building in different distributions or with older toolchains.
   While we don't support other linux distributions or GCC versions earlier
   than 11, this section provides some notes that may help you
   in that situation.

   * GCC 8 mixed with older versions of GNU as/binutils may fail to compile
     `roaring.c` with an error similar to:
     ```
     /tmp/cczCvQKd.s: Assembler messages:
     /tmp/cczCvQKd.s:45826: Error: no such instruction: `vpcompressb %zmm0,%zmm1{%k2}'
     /tmp/cczCvQKd.s:46092: Error: no such instruction: `vpcompressb %zmm0,%zmm1{%k1}'
     ```
     In that case, add `-DCMAKE_C_FLAGS=-DCROARING_COMPILER_SUPPORTS_AVX512=0`
     to the list of arguments to `cmake`.

   * Some platforms combining old versions of GCC and cmake may fail
     to set the cmake C++ standard to 17 without explicitly adding
     `-DCMAKE_CXX_STANDARD=17` to the list of arguments to `cmake`.
     Note the default mode for C++ is `-std=gnu++17` for GCC 11.

Notes
  (1) The standard assumptions for `Debug` and `Release` apply here.
      With a `Debug` build you get debug information which is useful during
      development and testing of your own code that depends on the client
      and indirectly on these libraries.  A `Release` build gives you
      optimized libraries that are faster and smaller but with no
      debugging information.  Note that while in general it is expected
      to be able to freely mix some `Debug` and `Release` code,
      some of the dependent libraries are incompatible; in particular,
      protobuf generates different code and code compiled for a `Release`
      target using protobuf header files will not link against a `Debug`
      version of protobuf.  To keep things simple, we suggest that you run
      a consistent setting for your code and all dependencies.
  (2) In Fedora, the packages needed for building:

      ```
      dnf -y groupinstall 'Development Tools'
      dnf -y install curl cmake gcc-c++ openssl-devel libcurl-devel
      ```

# Proto generated C++ stubs (intended for developers)

The protobuf and gRPC C++ stubs are not checked in. They are generated at build time by
`cpp-client/deephaven/dhclient/CMakeLists.txt` from the `.proto` sources in
`proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto`, into the build tree under
`<build-dir>/dhclient/proto`. There is nothing to regenerate by hand; a normal build picks up
any change to the `.proto` files. Adding a new `.proto` file requires adding it to the
`PROTO_FILES` list in that `CMakeLists.txt`.

The location of the `.proto` tree is the CMake cache variable `DH_PROTO_DIR`, which defaults to
its place in the deephaven-core checkout. The Nix build sets it to the `protoSrc` output of
`proto/proto-backplane-grpc/flake.nix`, a store path holding only the `.proto` files.
