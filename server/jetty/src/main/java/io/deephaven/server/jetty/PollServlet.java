package io.deephaven.server.jetty;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders5;
import io.deephaven.qst.table.NewTable;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class PollServlet extends HttpServlet {

    private static final ColumnHeaders5<Instant, String, String, String, Long> HEADERS = ColumnHeader.ofInstant("Timestamp")
            .header(ColumnHeader.ofString("Address"))
            .header(ColumnHeader.ofString("Id"))
            .header(ColumnHeader.ofString("UserAgent"))
            .header(ColumnHeader.ofLong("BestNumber"));

    private static volatile AppendOnlyArrayBackedMutableTable TABLE = null;

    private static AppendOnlyArrayBackedMutableTable table() {
        AppendOnlyArrayBackedMutableTable localT = TABLE;
        if ((localT = TABLE) == null) {
            synchronized (PollServlet.class) {
                if ((localT = TABLE) == null) {
                    localT = TABLE = AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADERS));
                }
            }
        }
        return localT;
    }

    private static void record(Instant timestamp, String remoteAddr, String id, String userAgent, long bestNumber) throws IOException {
        NewTable newTable = HEADERS.start(1).row(timestamp, remoteAddr, id, userAgent, bestNumber).newTable();
        table().mutableInputTable().add(InMemoryTable.from(newTable));
    }

    public static Table getTable() {
        return table().readOnlyCopy();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        // todo: is there a way to do this browser / js side?
        String host = req.getHeader("Host");

        response.setStatus(HttpServletResponse.SC_OK);
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html");
        Writer htmlWriter = new OutputStreamWriter(response.getOutputStream(), UTF_8);
        htmlWriter.append("<html><head><title>");
        htmlWriter.append("Poll");
        htmlWriter.append("</title></head><body>\n");
        htmlWriter.append("<form action=\"/poll\" method=\"post\">\n" +
                "  <label for=\"num\">What is the best number? </label>\n" +
                "  <input type=\"number\" id=\"num\" name=\"num\" value=\"42\"><br><br>\n" +
                "  <input type=\"submit\" value=\"Submit\">\n" +
                "</form>");
        htmlWriter.append("\nNote: \"advanced\" users can also run:");
        htmlWriter.append("\n<br><br>curl -d 'num=42' '").append(host).append("/poll'");
        htmlWriter.append("\n</body></html>\n");
        htmlWriter.flush();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        String num = req.getParameter("num");
        if (num == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        long bestNumber;
        try {
            bestNumber = Long.parseLong(num);
        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String remoteAddr = req.getRemoteAddr();
        String id = req.getSession().getId();
        String userAgent = req.getHeader("User-Agent");

        record(Instant.now(), remoteAddr, id, userAgent, bestNumber);

        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html");
        Writer htmlWriter = new OutputStreamWriter(response.getOutputStream(), UTF_8);
        htmlWriter.append("<html>");
        htmlWriter.append("\n  <head>");
        htmlWriter.append("\n    <title>Poll submitted</title>");
        htmlWriter.append("\n  </head>");
        htmlWriter.append("\n  <body>");
        htmlWriter.append("\n  Thanks for your poll submission. I agree, the best number is " + num + ".");
        htmlWriter.append("\n  <br><br>");
        htmlWriter.append("\n  <a href=\"/poll\">Back</a>");
        htmlWriter.append("\n  </body>");
        htmlWriter.append("\n</html>\n");
        htmlWriter.flush();
    }
}
