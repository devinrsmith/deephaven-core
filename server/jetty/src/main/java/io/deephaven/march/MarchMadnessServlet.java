package io.deephaven.march;

import io.deephaven.engine.table.Table;
import io.deephaven.function.IntegerPrimitives;
import jakarta.servlet.ServletException;
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
import java.util.concurrent.ThreadLocalRandom;

@Singleton
public final class MarchMadnessServlet extends HttpServlet {

    public static final String MARCH_MADNESS_ID = "MARCH_MADNESS_ID";
    private static MarchMadnessServlet INSTANCE;

    public static void init(MarchMadnessServlet x) {
        INSTANCE = x;
    }

    public static Table votes() {
        return INSTANCE.votes.getReadOnlyTable();
    }

    private final Bracket bracket;
    private final Votes votes;

    @Inject
    public MarchMadnessServlet(Bracket bracket, Votes votes) {
        this.bracket = Objects.requireNonNull(bracket);
        this.votes = Objects.requireNonNull(votes);
        init(this);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        String teamId = req.getParameter("teamId");
        if (teamId == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        int parsedTeamId;
        try {
            parsedTeamId = Integer.parseInt(teamId);
        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        if (IntegerPrimitives.isNull(parsedTeamId)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        final Team team = bracket.idToTeam().get(parsedTeamId);
        if (team == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }


        final Cookie[] cookies = req.getCookies();
        final Optional<Cookie> cookie = cookies == null ?
                Optional.empty() :
                Arrays.stream(cookies)
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


        final String xForwardedFor = req.getHeader("x-forwarded-for");
        final String remoteAddr = req.getRemoteAddr();
//        String session = req.getSession().getId();
        String userAgent = req.getHeader("User-Agent");
        final Vote vote = ImmutableVote.builder()
                .bracket(bracket)
                .team(team)
                .build();
        // todo: extract 64 bits
        votes.append(Instant.now(), xForwardedFor != null ? xForwardedFor : remoteAddr, marchSession, userAgent, vote);
        response.setStatus(HttpServletResponse.SC_CREATED);
        if (setCookie) {
            response.addCookie(new Cookie(MARCH_MADNESS_ID, Long.toString(marchSession)));
        }
    }
}
