package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public final class UriRouter {

    private final Set<UriResolver> resolvers;
    private final Config config;

    private final Map<String, Set<UriResolver>> map;

    @Inject
    public UriRouter(Set<UriResolver> resolvers, Config config) {
        this.resolvers = Objects.requireNonNull(resolvers);
        this.config = Objects.requireNonNull(config);
        map = new HashMap<>();
        for (UriResolver resolver : resolvers) {
            for (String scheme : resolver.schemes()) {
                final Set<UriResolver> set = map.computeIfAbsent(scheme, s -> new HashSet<>());
                set.add(resolver);
            }
        }
    }

    public Set<UriResolver> resolvers() {
        return resolvers;
    }

    public <T extends UriResolver> Optional<T> find(Class<T> clazz) {
        return resolvers()
                .stream()
                .filter(t -> clazz.equals(t.getClass()))
                .map(clazz::cast)
                .findFirst();
    }

    public UriResolver resolver(URI uri) {
        final List<UriResolver> resolvers = map.getOrDefault(uri.getScheme(), Collections.emptySet())
                .stream()
                .filter(t -> t.isResolvable(uri))
                .collect(Collectors.toList());
        if (resolvers.isEmpty()) {
            throw new UnsupportedOperationException(
                    String.format("Unable to find resolver for uri '%s'", uri));
        } else if (resolvers.size() > 1) {
            final String classes = resolvers.stream()
                    .map(UriResolver::getClass)
                    .map(Class::toString)
                    .collect(Collectors.joining(",", "[", "]"));
            throw new UnsupportedOperationException(
                    String.format("Found multiple resolvers for uri '%s': %s", uri, classes));
        }
        return resolvers.get(0);
    }

    public Object resolve(URI uri) throws InterruptedException {
        return resolver(uri).resolve(uri);
    }

    /**
     * Resolve the URI in a safe manner according to the general {@link Config configuration}.
     *
     * <p>
     * If {@link Config#isEnabled(AuthContext)} is {@code false}, no URIs can be resolved.
     *
     * <p>
     * If {@link Config#isEnabled(AuthContext, UriResolver)} is {@code false} for the resolver of {@code uri}, no URIs
     * can be resolved for that resolver.
     *
     * <p>
     * Returns {@link UriResolver#resolveSafely(AuthContext, URI)} for the resolver of {@code uri}.
     *
     * @param uri the URI
     * @return the resolved object
     */
    public Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        return resolverCheckConfig(auth, uri).resolveSafely(auth, uri);
    }

    private UriResolver resolverCheckConfig(AuthContext auth, URI uri) {
        if (!config.isEnabled(auth)) {
            throw new UnsupportedOperationException(
                    String.format("Deephaven URI resolvers are not enabled. %s", config.helpEnable(auth)));
        }
        final UriResolver resolver = resolver(uri);
        if (!config.isEnabled(auth, resolver)) {
            throw new UnsupportedOperationException(String.format("Deephaven URI resolver '%s' is not enabled. %s",
                    resolver.getClass(), config.helpEnable(auth, resolver)));
        }
        return resolver;
    }

    /**
     * The configuration for {@link UriRouter}. Useful as a general configuration layer for all resolvers. More specific
     * configuration will be based on the specific {@link UriResolver} implementation.
     */
    public interface Config {

        /**
         *
         * @return {@code true} if resolvers are enabled
         */
        boolean isEnabled(AuthContext auth);

        /**
         *
         * @param resolver the resolver
         * @return {@code true} if {@code resolver} is enabled
         */
        boolean isEnabled(AuthContext auth, UriResolver resolver);

        /**
         * A helper message to aid in enabling resolvers, {@link #isEnabled()}.
         *
         * @return the help message
         */
        String helpEnable(AuthContext auth);

        /**
         * A helper message to aid in enabling a specific resolvers, {@link #isEnabled(UriResolver)} for
         * {@code resolver}.
         *
         * @param resolver the resolver
         * @return the help message
         */
        String helpEnable(AuthContext auth, UriResolver resolver);
    }
}
