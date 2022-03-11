package io.deephaven.server.jetty;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static io.deephaven.server.jetty.PollServlet.getHost;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PollResultsServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html");
        Writer htmlWriter = new OutputStreamWriter(response.getOutputStream(), UTF_8);
        htmlWriter.append("<html>\n" +
                "<head>\n" +
                "  <title>Poll Results</title>\n" +
                "  <script src='https://cdn.plot.ly/plotly-2.9.0.min.js'></script>\n" +
                "  <script src='/jsapi/dh-internal.js'></script>\n" +
                "  <script src='/jsapi/dh-core.js'></script>\n" +
                "</head>\n" +
                "\n" +
                "<body>\n" +
                "  <div style=\"display: inline-block;\" id='poll_gauge'></div>\n" +
                "  <div style=\"display: inline-block;\" id='poll_results_pie'></div>\n" +
                "  <div style=\"display: inline-block;\" id='poll_results_bar'></div>\n" +
                "  <br><iframe src=\"/iframe/table/?name=global_stats\" height=\"80\" width=\"1000\" frameborder=\"0\"></iframe>\n" +
                "  <br><iframe src=\"/iframe/table/?name=poll_public\" height=\"500\" width=\"1280\" frameborder=\"0\"></iframe>\n" +
                "  <script>\n" +
                "async function initPie(session, table_name, div_name, title, height, width) {\n" +
                "  var data = \n" +
                "    {\n" +
                "      labels: [],\n" +
                "      values: [],\n" +
                "      type: 'pie'\n" +
                "    };\n" +
                "  var plotlyData = [data];\n" +
                "  \n" +
                "  var layout = {\n" +
                "    title: title,\n" +
                "    height: height,\n" +
                "    width: width\n" +
                "  };\n" +
                "  \n" +
                "  Plotly.newPlot(div_name, plotlyData, layout);\n" +
                "\n" +
                "  var table = await session.getObject({ name: table_name, type: dh.VariableType.TABLE });\n" +
                "  table.addEventListener(dh.Table.EVENT_UPDATED, event => {\n" +
                "    const viewportData = event.detail;\n" +
                "    const { columns } = viewportData;\n" +
                "    data.labels = [];\n" +
                "    data.values = [];\n" +
                "    for (let r = 0; r < viewportData.rows.length; r += 1) {\n" +
                "      const row = viewportData.rows[r];\n" +
                "      data.labels.push(`${row.get(columns[0])}`);\n" +
                "      data.values.push(`${row.get(columns[1])}`);\n" +
                "    }\n" +
                "    Plotly.react(div_name, plotlyData, layout);\n" +
                "  });  \n" +
                "  table.setViewport(0, 100);\n" +
                "}\n" +
                "\n" +
                "async function initBar(session, table_name, div_name, title, height, width) {\n" +
                "  var data = \n" +
                "    {\n" +
                "      x: [],\n" +
                "      y: [],\n" +
                "      type: 'bar'\n" +
                "    };\n" +
                "  var plotlyData = [data];  \n" +
                "  var layout = {\n" +
                "    title: title,\n" +
                "    height: height,\n" +
                "    width: width\n" +
                "  };  \n" +
                "  Plotly.newPlot(div_name, plotlyData, layout);\n" +
                "\n" +
                "  var table = await session.getObject({ name: table_name, type: dh.VariableType.TABLE });\n" +
                "  table.addEventListener(dh.Table.EVENT_UPDATED, event => {\n" +
                "    const viewportData = event.detail;\n" +
                "    const { columns } = viewportData;\n" +
                "    data.x = [];\n" +
                "    data.y = [];\n" +
                "    for (let r = 0; r < viewportData.rows.length; r += 1) {\n" +
                "      const row = viewportData.rows[r];\n" +
                "      data.x.push(`${row.get(columns[0])}`);\n" +
                "      data.y.push(`${row.get(columns[1])}`);\n" +
                "    }\n" +
                "    Plotly.react(div_name, plotlyData, layout);\n" +
                "  });  \n" +
                "  table.setViewport(0, 100);\n" +
                "}\n" +
                "\n" +
                "async function initGauge(session, table_name, table_max_name, div_name, title, height, width) { \n" +
                "  var data =\n" +
                "    {\n" +
                "      domain: { x: [0, 1], y: [0, 1] },\n" +
                "      value: 0,\n" +
                "      title: { text: title },\n" +
                "      type: \"indicator\",\n" +
                "      mode: \"gauge+number\",\n" +
                "      gauge: { axis: { range: [null, 100] } }\n" +
                "    };\n" +
                "  var plotlyData = [data]\n" +
                "  var layout = { width: width, height: height };\n" +
                "  Plotly.newPlot(div_name, plotlyData, layout);\n" +
                "  \n" +
                "  var table = await session.getObject({ name: table_name, type: dh.VariableType.TABLE });\n" +
                "  table.addEventListener(dh.Table.EVENT_UPDATED, event => {\n" +
                "    const viewportData = event.detail;\n" +
                "    const { columns } = viewportData;   \n" +
                "    for (let r = 0; r < viewportData.rows.length; r += 1) {\n" +
                "      const row = viewportData.rows[r];\n" +
                "      const val = `${row.get(columns[0])}`;\n" +
                "      data.value = val;\n" +
                "    }\n" +
                "    Plotly.react(div_name, plotlyData, layout);\n" +
                "  });  \n" +
                "  table.setViewport(0, 0);\n" +
                "  \n" +
                "  var table_max = await session.getObject({ name: table_max_name, type: dh.VariableType.TABLE });\n" +
                "  table_max.addEventListener(dh.Table.EVENT_UPDATED, event => {\n" +
                "    const viewportData = event.detail;\n" +
                "    const { columns } = viewportData;   \n" +
                "    for (let r = 0; r < viewportData.rows.length; r += 1) {\n" +
                "      const row = viewportData.rows[r];\n" +
                "      const val = `${row.get(columns[0])}`;\n" +
                "      data.gauge = { axis: { range: [null, val] } };\n" +
                "    }\n" +
                "    Plotly.react(div_name, plotlyData, layout);\n" +
                "  });  \n" +
                "  table_max.setViewport(0, 0);\n" +
                "}\n" +
                "\n" +
                "async function init(host) {\n" +
                "  var connection = new dh.IdeConnection(host) \n" +
                "  var session = await connection.startSession('groovy');\n" +
                "  initPie(session, 'poll_count_top_10', 'poll_results_pie', 'Poll Results (top 10)', 400, 500);\n" +
                "  initBar(session, 'poll_count_top_5', 'poll_results_bar', 'Poll Results (top 5)', 400, 800);\n" +
                "  initGauge(session, 'hits_by_min_latest', 'hits_by_min_max', 'poll_gauge', 'Hits this minute', 400, 400);\n" +
                "}\n" +
                "\n" +
                "init('" + getHost() + "')\n" +
                "  </script>\n" +
                "</body>\n" +
                "</html>");
        htmlWriter.flush();
    }
}
