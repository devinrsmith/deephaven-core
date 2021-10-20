package io.deephaven.uri;

import java.nio.file.Path;

class UriHelper {
    static DeephavenUri of(Path path) {
        final DeephavenUri uri = fromInternal(path);
        if (!path.equals(uri.path())) {
            throw new IllegalStateException(
                    String.format("Parsing code is not symmetrical, unable to match path '%s'", path));
        }
        return uri;
    }

    static DeephavenUri of(DeephavenTarget target, Path path) {
        final DeephavenUri uri = fromInternal(target, path);
        if (!path.equals(uri.path())) {
            throw new IllegalStateException(
                    String.format("Parsing code is not symmetrical, unable to match path '%s'", path));
        }
        return uri;
    }

    private static DeephavenUri fromInternal(Path path) {
        if (DeephavenUriQueryScope.isMatch(path)) {
            return DeephavenUriQueryScope.builder().parse(path).build();
        }
        if (DeephavenUriApplicationField.isMatch(path)) {
            return DeephavenUriApplicationField.builder().parse(path).build();
        }
        if (DeephavenUriField.isMatch(path)) {
            return DeephavenUriField.builder().parse(path).build();
        }
        if (DeephavenUriProxy.isMatch(path)) {
            return DeephavenUriProxy.builder().parse(path).build();
        }
        throw new IllegalArgumentException();
    }

    private static DeephavenUri fromInternal(DeephavenTarget target, Path path) {
        if (DeephavenUriQueryScope.isMatch(path)) {
            return DeephavenUriQueryScope.builder().target(target).parse(path).build();
        }
        if (DeephavenUriApplicationField.isMatch(path)) {
            return DeephavenUriApplicationField.builder().target(target).parse(path).build();
        }
        if (DeephavenUriField.isMatch(path)) {
            return DeephavenUriField.builder().target(target).parse(path).build();
        }
        if (DeephavenUriProxy.isMatch(path)) {
            return DeephavenUriProxy.builder().target(target).parse(path).build();
        }
        throw new IllegalArgumentException();
    }
}
