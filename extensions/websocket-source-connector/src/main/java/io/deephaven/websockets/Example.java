/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.engine.table.Table;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.StringOptions;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class Example {

    static Table coinbaseTicker(Collection<String> productIds) throws Exception {
        return WebsocketTable.builder()
                .uri(URI.create("wss://ws-feed.exchange.coinbase.com"))
                .stringProcessor(coinbaseTicker())
                .addSubscribeMessages(coinbaseSubscribe(productIds, List.of("ticker")))
                .skipFirstN(1) // skip sub response
                .build()
                .execute();
    }

    static Table coinbaseMatches(Collection<String> productIds) throws Exception {
        return WebsocketTable.builder()
                .uri(URI.create("wss://ws-feed.exchange.coinbase.com"))
                .stringProcessor(coinbaseMatch())
                .addSubscribeMessages(coinbaseSubscribe(productIds, List.of("matches")))
                .skipFirstN(1) // skip sub response
                .build()
                .execute();
    }

    // public static NamedObjectProcessor<String> processor(ValueOptions opts) {
    // return new ObjectProcessorJsonValue.ObjectProcessorJsonValueString(new JsonFactory(), opts).named();
    // }

    private static String toJsonArray(Collection<String> elements) {
        return elements.stream().collect(Collectors.joining("\",\"", "[\"", "\"]"));
    }

    private static String coinbaseSubscribe(Collection<String> productIds, Collection<String> channels) {
        return String.format(
                "{\"type\":\"subscribe\",\"product_ids\":%s,\"channels\":%s}",
                toJsonArray(productIds),
                toJsonArray(channels));
    }

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
}
