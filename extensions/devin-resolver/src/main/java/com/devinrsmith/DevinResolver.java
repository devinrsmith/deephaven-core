package com.devinrsmith;

import io.deephaven.grpc_api.uri.AuthorizationScope;
import io.deephaven.grpc_api.uri.Authorization;
import io.deephaven.grpc_api.uri.UriResolverBase;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DevinResolver extends UriResolverBase<URI> {
    public static final Set<String> SCHEMES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("dh", "dh+plain")));

    public static final String EXPECTED_PATH = "/devin";

    public static final Pattern QUERY_PATTERN = Pattern.compile("^foo=(.+)&bar=(.+)$");

    @Inject
    public DevinResolver() {}

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        return uri.getHost() == null
                && !uri.isOpaque()
                && EXPECTED_PATH.equals(uri.getPath())
                && uri.getQuery() != null
                && QUERY_PATTERN.matcher(uri.getQuery()).matches()
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    @Override
    public URI adaptToPath(URI uri) {
        return uri;
    }

    @Override
    public URI adaptToUri(URI path) {
        return path;
    }

    @Override
    public Object resolvePath(URI path) {
        if (!EXPECTED_PATH.equals(path.getPath())) {
            throw new IllegalStateException();
        }
        final Matcher matcher = QUERY_PATTERN.matcher(path.getQuery());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final List<Integer> fooGroup =
                Stream.of(matcher.group(1).split(",")).map(DevinResolver::map).collect(Collectors.toList());
        final List<Integer> barGroup =
                Stream.of(matcher.group(2).split(",")).map(DevinResolver::map).collect(Collectors.toList());

        // DevinResolver encapsulates URI format for calling FooBarLogic.fooBar
        return FooBarLogic.fooBar(fooGroup, barGroup);
    }

    @Override
    public void forAllPaths(BiConsumer<URI, Object> consumer) {

    }

    @Override
    public void forPaths(Predicate<URI> predicate, BiConsumer<URI, Object> consumer) {

    }

    private static Integer map(String input) {
        return input == null || input.isEmpty() ? null : Integer.parseInt(input);
    }

    @Override
    public Authorization<URI> authorization(AuthorizationScope<URI> scope, AuthContext context) {
        return Authorization.allow(scope);
    }
}
