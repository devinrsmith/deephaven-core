package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;

/**
 * A Deephaven proxy URI.
 *
 * <p>
 * For example, {@code dh://gateway/dh/internal-host/s/my_table}.
 */
@Immutable
@BuildableStyle
public abstract class DeephavenUriProxy extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriProxy.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() > 2
                && (TLS_PROXY.equals(path.getName(0)) || PLAIN_PROXY.equals(path.getName(0)));
    }

    /**
     * The inner Deephaven URI.
     *
     * @return the inner Deephaven URI
     */
    public abstract DeephavenUri innerUri();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    /**
     * Equivalent to {@code ${innerUri.scheme}/${innerUri.authority}/${innerUri.path}}.
     *
     * @return the path
     */
    @Override
    public final Path path() {
        final DeephavenTarget innerTarget = innerUri().target().orElseThrow(IllegalStateException::new);
        final String innerAuthority = innerTarget.authority();
        final Path innerPath = innerUri().path();
        return (innerTarget.isTLS() ? TLS_PROXY : PLAIN_PROXY).resolve(innerAuthority).resolve(innerPath);
    }

    @Check
    final void checkInnerHost() {
        if (innerUri().isLocal()) {
            throw new IllegalArgumentException("Inner URI must be remote");
        }
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder innerUri(DeephavenUri innerUri);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }

            final DeephavenTarget.Builder targetBuilder = DeephavenTarget.builder();
            if (TLS_PROXY.equals(path.getName(0))) {
                targetBuilder.isTLS(true);
            } else if (PLAIN_PROXY.equals(path.getName(0))) {
                targetBuilder.isTLS(false);
            } else {
                throw new IllegalStateException();
            }
            final Path proxyPath = path.getName(1);
            final String[] targetParts = proxyPath.toString().split(":");

            final DeephavenTarget target;
            if (targetParts.length == 1) {
                target = targetBuilder.host(targetParts[0]).build();
            } else if (targetParts.length == 2) {
                target = targetBuilder.host(targetParts[0]).port(Integer.parseInt(targetParts[1])).build();
            } else {
                throw new IllegalArgumentException(String.format("Invalid proxy path '%s'", proxyPath));
            }

            final Path rest = path.subpath(2, path.getNameCount());

            return innerUri(DeephavenUri.of(target, rest));
        }

        DeephavenUriProxy build();
    }
}
