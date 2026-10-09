{
  description = "Deephaven gRPC / Barrage .proto sources";

  # No inputs on purpose: this flake only republishes source files, so consumers
  # (e.g. cpp-client/flake.nix) take it as an input without inheriting a nixpkgs pin.
  outputs = { self }: {
    # Root of the .proto tree, i.e. the directory containing deephaven_core/.
    # A separate store path holding only these files, so dependents rebuild when
    # the protos change and not on every unrelated change in the repository.
    protoSrc = builtins.path {
      name = "deephaven-proto";
      path = ./src/main/proto;
    };
  };
}
