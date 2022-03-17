package io.deephaven.march;

import dagger.Lazy;
import io.deephaven.function.IntegerPrimitives;
import io.deephaven.march.ImmutableVote.Builder;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public final class MarchMadnessServlet extends HttpServlet {

    private static final String MARCH_MADNESS_ID = "MARCH_MADNESS_ID";

    private final Lazy<Votes> votes;

    @Inject
    public MarchMadnessServlet(Lazy<Votes> votes) {
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
            badResponse(response, "Bad or missing 'roundOf'");
            return;
        }
        final OptionalInt teamId = parseInt(req, "teamId");
        if (teamId.isEmpty()) {
            badResponse(response, "Bad or missing 'teamId'");
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
        long marchSession;
        boolean setCookie;
        if (cookie.isPresent()) {
            try {
                marchSession = Long.parseLong(cookie.get().getValue());
                setCookie = false;
            } catch (NumberFormatException e) {
                marchSession = ThreadLocalRandom.current().nextLong();
                setCookie = true;
            }
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

        boolean mismatched = votes.get().append(builder.build());

        if (setCookie) {
            response.addCookie(new Cookie(MARCH_MADNESS_ID, Long.toString(marchSession)));
        }
        if (mismatched) {
            badResponse(response, "Mismatched 'roundOf' and/or 'teamId'");
        } else {
            response.setStatus(HttpServletResponse.SC_CREATED);
        }
    }

    private static void badResponse(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/plain");
        try (final Writer writer = new OutputStreamWriter(response.getOutputStream(), UTF_8)) {
            writer.write(message);
        }
    }
}
