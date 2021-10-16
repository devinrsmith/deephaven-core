package io.deephaven;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.qst.table.TicketTable;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;

@Immutable
@SimpleStyle
public abstract class DeephavenUri {

    public static DeephavenUri of(String uri) {
        return of(URI.create(uri));
    }

    public static DeephavenUri of(String authority, TicketTable ticket) {
        if (authority == null) {
            return of(URI.create(String.format("dh:///%s", new String(ticket.ticket(), StandardCharsets.UTF_8))));
        }
        final DeephavenUri uri = of(URI
                .create(String.format("dh://%s/%s", authority, new String(ticket.ticket(), StandardCharsets.UTF_8))));
        if (!uri.ticket().equals(ticket)) {
            throw new IllegalArgumentException(String.format("Invalid authority '%s'", authority));
        }
        return uri;
    }

    public static DeephavenUri of(String host, int port, TicketTable ticket) {
        if (host == null) {
            throw new NullPointerException("Host must be non-null");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Invalid port " + port);
        }
        final DeephavenUri uri = of(URI.create(
                String.format("dh://%s:%d/%s", host, port, new String(ticket.ticket(), StandardCharsets.UTF_8))));
        if (!host.equals(uri.host().orElse(null))) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host));
        }
        return uri;
    }

    public static DeephavenUri of(URI uri) {
        return ImmutableDeephavenUri.of(uri);
    }

    @Parameter
    public abstract URI uri();

    public final Optional<String> host() {
        return Optional.ofNullable(uri().getHost());
    }

    public final OptionalInt port() {
        return uri().getPort() == -1 ? OptionalInt.empty() : OptionalInt.of(uri().getPort());
    }

    public final TicketTable ticket() {
        return TicketTable.of(uri().getPath().substring(1));
    }

    public final boolean isLocal() {
        return uri().getHost() == null;
    }

    @Check
    final void checkScheme() {
        if (!"dh".equals(uri().getScheme())) {
            throw new IllegalArgumentException(String.format("Invalid scheme '%s', must be 'dh'", uri().getScheme()));
        }
    }

    @Check
    final void checkNonOpaque() {
        if (uri().isOpaque()) {
            throw new IllegalArgumentException("Deephavhen uri must not be opaque");
        }
    }

    @Check
    final void checkAbsolutePath() {
        if (uri().getPath() == null || uri().getPath().charAt(0) != '/') {
            throw new IllegalArgumentException("Deephavhen uri path must be absolute");
        }
    }

    @Check
    final void checkUserInfo() {
        if (uri().getUserInfo() != null) {
            throw new IllegalArgumentException("Deephaven uri does not support user info at this time");
        }
    }

    @Check
    final void checkQuery() {
        if (uri().getQuery() != null) {
            throw new IllegalArgumentException("Deephaven uri does not support query params at this time");
        }
    }

    @Check
    final void checkFragment() {
        if (uri().getFragment() != null) {
            throw new IllegalArgumentException("Deephaven uri does not support fragments at this time");
        }
    }

    @Check
    final void checkValidTicket() {
        ticket();
    }
}
