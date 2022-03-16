package io.deephaven.march;

import dagger.Lazy;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.function.IntegerPrimitives;
import io.deephaven.march.ImmutableVote.Builder;
import io.deephaven.util.locks.AwareFunctionalLock;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;

@Singleton
public final class MarchMadnessServlet extends HttpServlet {

    private static final String MARCH_MADNESS_ID = "MARCH_MADNESS_ID";

    private final Lazy<Matches> matches;
    private final Lazy<Votes> votes;

    @Inject
    public MarchMadnessServlet(Lazy<Matches> matches, Lazy<Votes> votes) {
        this.matches = Objects.requireNonNull(matches);
        this.votes = Objects.requireNonNull(votes);
    }

    private static OptionalInt parseInt(ServletRequest request, String parameterName) {
        String value = request.getParameter(parameterName);
        if (value == null) {
            return OptionalInt.empty();
        }
        final int intValue;
        try {
            intValue = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
        if (IntegerPrimitives.isNull(intValue)) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(intValue);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        final OptionalInt roundOf = parseInt(req, "roundOf");
        if (roundOf.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        final OptionalInt teamId = parseInt(req, "teamId");
        if (teamId.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        final String xForwardedFor = req.getHeader("x-forwarded-for");
        final String userAgent = req.getHeader("User-Agent");
        final String remoteAddr = req.getRemoteAddr();
        final Cookie[] cookies = req.getCookies();
        final Optional<Cookie> cookie = cookies == null ? Optional.empty()
                : Arrays.stream(cookies)
                        .filter(c -> MARCH_MADNESS_ID.equals(c.getName()))
                        .findFirst();
        final long marchSession;
        final boolean setCookie;
        if (cookie.isPresent()) {
            marchSession = Long.parseLong(cookie.get().getValue());
            setCookie = false;
        } else {
            marchSession = ThreadLocalRandom.current().nextLong();
            setCookie = true;
        }

        final Builder builder = ImmutableVote.builder()
                .roundOf(roundOf.getAsInt())
                .teamId(teamId.getAsInt())
                .timestamp(Instant.now())
                .ip(xForwardedFor != null ? xForwardedFor : remoteAddr)
                .session(marchSession);
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }

        // final Lock readLock = matches.get().readLock();
        // readLock.lock();
        final AwareFunctionalLock lock = UpdateGraphProcessor.DEFAULT.exclusiveLock();
        lock.lock();
        try {
            final OptionalInt matchIx = matches.get().isValid(roundOf.getAsInt(), teamId.getAsInt());
            if (matchIx.isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            votes.get().append(builder.matchIndex(matchIx.getAsInt()).build());
        } finally {
            lock.unlock();
        }
        response.setStatus(HttpServletResponse.SC_CREATED);
        if (setCookie) {
            response.addCookie(new Cookie(MARCH_MADNESS_ID, Long.toString(marchSession)));
        }
    }
}
