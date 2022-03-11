import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggCountDistinct
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggMed

import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

poll_csv = readCsv(System.getProperty("poll.file", "poll.csv"), CsvSpecs.builder()
        .hasHeaderRow(false)
        .putParserForName("Timestamp", io.deephaven.csv.parsers.DateTimeAsLongParser.INSTANCE)
        .putParserForName("Address", io.deephaven.csv.parsers.StringParser.INSTANCE)
        .putParserForName("Id", io.deephaven.csv.parsers.StringParser.INSTANCE)
        .putParserForName("UserAgent", io.deephaven.csv.parsers.StringParser.INSTANCE)
        .putParserForName("BestNumber", io.deephaven.csv.parsers.LongParser.INSTANCE)
        .headers(["Timestamp" ,"Address", "Id", "UserAgent", "BestNumber"])
        .build())

poll = merge(poll_csv, io.deephaven.server.jetty.PollServlet.getTable())
poll_count = poll.countBy("Count", "BestNumber").sortDescending("Count")
poll_count_top_5 = poll_count
        .head(5)
        .view("Category=`C`+BestNumber", "Count")
poll_count_rest = poll_count
        .slice(5, Long.MAX_VALUE>>1)
        .view("Count")
        .sumBy()
        .view("Category=`Others`", "Count")
poll_results = merge(poll_count_top_5, poll_count_rest)

global_stats = poll.aggBy([
        AggCount("Count"),
        AggCountDistinct("UniqueAddresses=Address"),
        AggCountDistinct("UniqueIds=Id"),
        AggCountDistinct("UniqueAgents=UserAgent"),
        AggCountDistinct("UniqueNumbers=BestNumber"),
        AggMin("MinNumber=BestNumber"),
        AggMax("MaxNumber=BestNumber"),
        AggAvg("AvgNumber=BestNumber"),
        AggMed("MedNumber=BestNumber")])

poll_by_number = poll.aggBy([AggCountDistinct("UniqueAddresses=Address")], "BestNumber").sortDescending("UniqueAddresses")
poll_by_number_top_5 = poll_by_number
        .head(5)
        .view("Category=`C`+BestNumber", "UniqueAddresses")
poll_by_number_rest = poll_by_number
        .slice(5, Long.MAX_VALUE>>1)
        .view("UniqueAddresses")
        .sumBy()
        .view("Category=`Others`", "UniqueAddresses")
poll_by_number_results = merge(poll_by_number_top_5, poll_by_number_rest)
