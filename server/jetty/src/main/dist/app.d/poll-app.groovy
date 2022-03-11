import io.deephaven.csv.parsers.DateTimeAsLongParser
import io.deephaven.csv.parsers.LongParser
import io.deephaven.csv.parsers.StringParser
import io.deephaven.server.jetty.PollServlet
import io.deephaven.csv.CsvSpecs

import static io.deephaven.api.agg.Aggregation.AggCount
import static io.deephaven.api.agg.Aggregation.AggCountDistinct
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggMed
import static io.deephaven.engine.util.TableTools.merge
import static io.deephaven.csv.CsvTools.readCsv


poll_csv = readCsv(System.getProperty("poll.file", "poll.csv"), CsvSpecs.builder()
        .hasHeaderRow(false)
        .putParserForName("Timestamp", DateTimeAsLongParser.INSTANCE)
        .putParserForName("Address", StringParser.INSTANCE)
        .putParserForName("Id", StringParser.INSTANCE)
        .putParserForName("UserAgent", StringParser.INSTANCE)
        .putParserForName("BestNumber", LongParser.INSTANCE)
        .headers(["Timestamp" ,"Address", "Id", "UserAgent", "BestNumber"])
        .build())

poll = merge(poll_csv, PollServlet.getTable())

poll_count = poll.countBy("Count", "BestNumber")
        .sortDescending("Count")

poll_count_top_5 = poll_count
        .head(5)
        .view("Category=`C`+BestNumber", "Count")

poll_count_top_10 = poll_count
        .head(10)
        .view("Category=`C`+BestNumber", "Count")

hits_by_minute = poll
        .view("TimestampBin=lowerBin(Timestamp, 60000000000)")
        .countBy("Count", "TimestampBin")
        .sortDescending("TimestampBin")
hits_by_min_latest = hits_by_minute.head(1).view("Count")
hits_by_min_max = hits_by_minute.view("Count").maxBy()

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
