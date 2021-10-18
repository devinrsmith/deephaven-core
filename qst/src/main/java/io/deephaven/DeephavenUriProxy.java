package io.deephaven;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;
import java.nio.file.Paths;

@Immutable
@BuildableStyle
public abstract class DeephavenUriProxy extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriProxy.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() > 2 && PROXY.equals(path.getName(0));
    }

    public abstract DeephavenUriI innerUri();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final Path path() {
        return PROXY.resolve(proxyPath()).resolve(innerUri().path());
    }

    private Path proxyPath() {
        final Path proxyPath;
        if (innerUri().port().isPresent()) {
            proxyPath = Paths.get(innerUri().host().get() + ":" + innerUri().port().getAsInt());
        } else {
            proxyPath = Paths.get(innerUri().host().get());
        }
        return proxyPath;
    }

    @Check
    final void checkInnerHost() {
        if (!innerUri().host().isPresent()) {
            throw new IllegalArgumentException("Inner uri must have host");
        }
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder innerUri(DeephavenUriI innerUri);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }
            final Path proxyPath = path.getName(1);
            final String[] targetParts = proxyPath.toString().split(":");

            final Path rest = path.subpath(2, path.getNameCount());

            if (targetParts.length == 1) {
                return innerUri(DeephavenUriI.from(targetParts[0], rest));
            }
            if (targetParts.length == 2) {
                return innerUri(DeephavenUriI.from(targetParts[0], Integer.parseInt(targetParts[1]), rest));
            }
            throw new IllegalArgumentException();
        }

        DeephavenUriProxy build();
    }
}
