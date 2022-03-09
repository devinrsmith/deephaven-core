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

    private static final ColumnHeaders5<Instant, String, String, String, Long> HEADERS =
            ColumnHeader.ofInstant("Timestamp")
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

    private static void record(Instant timestamp, String remoteAddr, String id, String userAgent, long bestNumber)
            throws IOException {
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
        htmlWriter.append("\n    <title>Poll submitted</title>" +
                "  <script src='https://cdn.plot.ly/plotly-2.9.0.min.js'></script>\n" +
                "  <script src='https://crypto.devinrsmith.com/jsapi/dh-internal.js'></script>\n" +
                "  <script src='https://crypto.devinrsmith.com/jsapi/dh-core.js'></script>");
        htmlWriter.append("\n  </head>");
        htmlWriter.append("\n  <body>");
        htmlWriter.append("\n  Thanks for your poll submission. I agree, the best number is " + num + ".");
        htmlWriter.append("\n  <br><br>");
        htmlWriter.append("\n  <a href=\"/poll\">Back</a>");
        htmlWriter.append("\n  <br><br>");
//        htmlWriter.append("\n  <iframe src=\"/iframe/table/?name=poll_results\" height=\"300\" width=\"1280\" frameborder=\"0\"></iframe>");
        htmlWriter.append("\n  <div id='myDiv'><!-- Plotly chart will be drawn inside this DIV --></div>");
        htmlWriter.append("\n");
        htmlWriter.append("<script>\n" +
                "  var HOST = 'http://localhost:10000';\n" +
                "var TABLE_NAME = 'poll_results';\n" +
                "\n" +
                "var data = \n" +
                "  {\n" +
                "    x: [],\n" +
                "    y: [],\n" +
                "    type: 'bar'\n" +
                "  };\n" +
                "var plotlyData = [data];\n" +
                "\n" +
                "\n" +
                "async function initTable() {\n" +
                "  console.log('Creating connection');\n" +
                "  var connection = new dh.IdeConnection(HOST)\n" +
                "\n" +
                "  console.log('Creating session');\n" +
                "  var session = await connection.startSession('python');\n" +
                "  \n" +
                "  console.log('Loading table', TABLE_NAME);\n" +
                "  var table = await session.getObject({ name: TABLE_NAME, type: dh.VariableType.TABLE });\n" +
                "  \n" +
                "  console.log('Adding listener'); table.addEventListener(dh.Table.EVENT_UPDATED, event => {\n" +
                "    console.log('Table viewport updated, extracting data');\n" +
                "    // Extract the viewport data\n" +
                "    const viewportData = event.detail;\n" +
                "    const { columns } = viewportData;\n" +
                "    \n" +
                "    // New axis arrays for storing the data. Just replace the old one\n" +
                "    data.x = [];\n" +
                "    data.y = [];\n" +
                "    for (let r = 0; r < viewportData.rows.length; r += 1) {\n" +
                "      const row = viewportData.rows[r];\n" +
                "      // Assumes column[0] is x, column[1] is y\n" +
                "      // Convert to a string so that if one of the columns is a long wrapper, it gets converted automatically\n" +
                "      // Could also use value.asNumber(), but this way we don't need to check, and plotly doesn't care if it's a string or a number\n" +
                "      data.x.push(`${row.get(columns[0])}`);\n" +
                "      data.y.push(`${row.get(columns[1])}`);\n" +
                "    }\n" +
                "    \n" +
                "    console.log('Viewport data extracted', plotlyData);\n" +
                "    Plotly.react('myDiv', plotlyData);\n" +
                "  });\n" +
                "  \n" +
                "  table.setViewport(0, table.size);\n" +
                "}\n" +
                "\n" +
                "Plotly.newPlot('myDiv', plotlyData);\n" +
                "\n" +
                "initTable();\n" +
                "</script>");
        htmlWriter.append("\n  </body>");
        htmlWriter.append("\n</html>\n");
        htmlWriter.flush();
    }
}
