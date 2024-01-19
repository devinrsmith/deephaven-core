/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ObjectProcessorJsonValueFromString;
import io.deephaven.json.StringOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class Example {

    // {"type":"ticker","sequence":71719158645,"product_id":"BTC-USD","price":"40813.37","open_24h":"42637.36","volume_24h":"24498.81475200","low_24h":"40600.05","high_24h":"42877.22","volume_30d":"516384.79738056","best_bid":"40809.65","best_bid_size":"0.10002360","best_ask":"40818.25","best_ask_size":"0.08964000","side":"sell","time":"2024-01-18T21:32:54.368300Z","trade_id":596784415,"last_size":"0.0006684"}

    static ObjectOptions coinbaseMatch() {
        return ObjectOptions.builder()
                .putFields("type", StringOptions.standard())
                .putFields("trade_id", LongOptions.standard())
                .putFields("sequence", LongOptions.standard())
                .putFields("maker_order_id", StringOptions.standard())
                .putFields("taker_order_id", StringOptions.standard())
                .putFields("time", InstantOptions.standard())
                .putFields("product_id", StringOptions.standard())
                .putFields("size", DoubleOptions.lenient())
                .putFields("price", DoubleOptions.lenient())
                .putFields("side", StringOptions.standard())
                .build();
    }

    static ObjectOptions coinbaseTicker() {
        return ObjectOptions.builder()
                // todo: verify ticker?
                .putFields("type", StringOptions.standard())
                .putFields("sequence", LongOptions.standard())
                // todo: sub-field parsing?
                .putFields("product_id", StringOptions.standard())
                .putFields("price", DoubleOptions.lenient())
                .putFields("open_24h", DoubleOptions.lenient())
                .putFields("volume_24h", DoubleOptions.lenient())
                .putFields("low_24h", DoubleOptions.lenient())
                .putFields("high_24h", DoubleOptions.lenient())
                .putFields("volume_30d", DoubleOptions.lenient())
                .putFields("best_bid", DoubleOptions.lenient())
                .putFields("best_bid_size", DoubleOptions.lenient())
                .putFields("best_ask", DoubleOptions.lenient())
                .putFields("best_ask_size", DoubleOptions.lenient())
                // todo: enum?
                .putFields("side", StringOptions.standard())
                .putFields("time", InstantOptions.standard())
                .putFields("trade_id", LongOptions.standard())
                .putFields("last_size", DoubleOptions.lenient())
                .build();
    }

    static PublishersOptions coinbase() {
        return PublishersOptions.builder()
                .uri(URI.create("wss://ws-feed.exchange.coinbase.com"))
                .processor(new ObjectProcessorJsonValueFromString(new JsonFactory(), coinbaseTicker()))
                .subscribeMessage(
                        "{\n" +
                        "      \"type\": \"subscribe\",\n" +
                        "      \"product_ids\": [\n" +
                        "        \"BTC-USD\"\n" +
                        "      ],\n" +
                        "      \"channels\": [\"ticker\"]\n" +
                        "    }")
                .build();
    }

    static PublishersOptions coinbase2() {
        return PublishersOptions.builder()
                .uri(URI.create("wss://ws-feed.exchange.coinbase.com"))
                .processor(new ObjectProcessorJsonValueFromString(new JsonFactory(), coinbaseMatch()))
                .subscribeMessage(
                        "{\n" +
                                "      \"type\": \"subscribe\",\n" +
                                "      \"product_ids\": [\n" +
                                "        \"BTC-USD\"\n" +
                                "      ],\n" +
                                "      \"channels\": [\"matches\"]\n" +
                                "    }")
                .build();
    }


    static Table test(PublishersOptions options) throws Exception {
        final PublishersOptions.ListenerImpl publisher = options.publisher();
        final List<String> autoNames = NamedObjectProcessor.prefix(options.processor(), "Auto").columnNames();
        final TableDefinition tableDef = TableDefinition.from(autoNames, options.processor().outputTypes());
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDef, publisher, ExecutionContext.getContext().getUpdateGraph(), "todo", Map.of());
        publisher.start();
        // todo: close if start throws
        return BlinkTableTools.blinkToAppendOnly(adapter.table());
    }

    static Table test() throws Exception {
        final ObjectProcessorJsonValueFromString processor = new ObjectProcessorJsonValueFromString(new JsonFactory(), coinbaseMatch());
        final PublishersOptions options = PublishersOptions.builder()
                .uri(URI.create("wss://ws-feed.exchange.coinbase.com"))
                .processor(processor)
                .subscribeMessage(
                        "{\n" +
                                "      \"type\": \"subscribe\",\n" +
                                "      \"product_ids\": [\n" +
                                "        \"BTC-USD\"\n" +
                                "      ],\n" +
                                "      \"channels\": [\"matches\"]\n" +
                                "    }")
                .skipFirstN(2) // skip sub response and last_match
                .build();
        final PublishersOptions.ListenerImpl publisher = options.publisher();
        final TableDefinition tableDef = TableDefinition.from(processor.named().columnNames(), processor.outputTypes());
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDef, publisher, ExecutionContext.getContext().getUpdateGraph(), "todo", Map.of());
        publisher.start();
        // todo: close if start throws
        return BlinkTableTools.blinkToAppendOnly(adapter.table());

    }
}
