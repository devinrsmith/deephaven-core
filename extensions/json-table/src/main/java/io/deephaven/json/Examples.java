/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.table.Table;
import io.deephaven.json.jackson.JacksonTable;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;

public class Examples {
    public static Table largeFile(String source) {
        return JacksonTable.of(JsonTableOptions.builder()
                .options(ArrayOptions.strict(ObjectOptions.builder()
                        .putFields("id", LongOptions.lenient())
                        .putFields("type", StringOptions.strict())
                        .putFields("created_at", InstantOptions.strict())
                        .build()))
                .addSources(Path.of(source))
                .build());
    }

    public static Table execute(String source) {
        return JacksonTable.of(JsonTableOptions.builder()
                .options(ObjectOptions.builder()
                        .putFields("id", StringOptions.strict())
                        .putFields("age", IntOptions.strict())
                        .build())
                .addSources(Path.of(source))
                .multiValueSupport(true)
                .build());
    }

    public static Table unconfirmedTransactions() throws MalformedURLException {
        return JacksonTable.of(JsonTableOptions.builder()
                .addSources(new URL("https://blockchain.info/unconfirmed-transactions?format=json"))
                .options(ObjectOptions.builder()
                        .putFields("txs", ArrayOptions.builder()
                                .element(ObjectOptions.builder()
                                        .putFields("hash", StringOptions.strict())
                                        .putFields("ver", IntOptions.strict())
                                        .putFields("vin_sz", IntOptions.strict())
                                        .putFields("vout_sz", IntOptions.strict())
                                        .putFields("size", IntOptions.strict())
                                        .putFields("weight", IntOptions.strict())
                                        .putFields("fee", LongOptions.strict())
                                        .putFields("relayed_by", StringOptions.strict())
                                        .putFields("lock_time", LongOptions.strict())
                                        .putFields("tx_index", LongOptions.strict())
                                        // todo double_spend bool
                                        .putFields("time", InstantNumberOptions.Format.EPOCH_SECONDS.strict())
                                        // todo: inputs
                                        // todo: outputs
                                        .build())
                                .build())
                        .build())
                .multiValueSupport(false)
                .build());
    }

    public static Table latestBlock() throws MalformedURLException {
        return JacksonTable.of(JsonTableOptions.builder()
                .addSources(new URL("https://blockchain.info/latestblock"))
                .options(ObjectOptions.builder()
                        .putFields("txIndexes", ArrayOptions.strict(LongOptions.strict()))
                        .build())
                .build());
    }

    // https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json
    public static Table dulux() throws MalformedURLException {
        return JacksonTable.of(JsonTableOptions.builder()
                .options(ArrayOptions.strict(ObjectOptions.builder()
                        .putFields("name", StringOptions.strict())
                        .putFields("code", StringOptions.strict())
                        .putFields("lrv", StringOptions.strict())
                        .putFields("id", LongOptions.lenient())
                        .putFields("r", IntOptions.lenient())
                        .putFields("g", IntOptions.lenient())
                        .putFields("b", IntOptions.lenient())
                        .build()))
                .addSources(new URL("https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json"))
                .build());
    }

    public static Table cloudTrails() {
        return JacksonTable.of(JsonTableOptions.builder()
                .options(ObjectOptions.builder()
                        .putFields("Records", ArrayOptions.strict(ObjectOptions.builder()
                                .putFields("userAgent", StringOptions.standard())
                                .putFields("eventID", StringOptions.strict())
                                .putFields("userIdentity", ObjectOptions.builder()
                                        .putFields("type", StringOptions.standard())
                                        .putFields("principalId", StringOptions.standard())
                                        .putFields("arn", StringOptions.standard())
                                        .putFields("accountId", StringOptions.standard())
                                        .putFields("accessKeyId", StringOptions.standard())
                                        .putFields("sessionContext", ObjectOptions.builder()
                                                .putFields("attributes", ObjectOptions.builder()
                                                        // todo bool lenient
                                                        .putFields("mfaAuthenticated", StringOptions.standard())
                                                        .putFields("creationDate", InstantOptions.standard())
                                                        .build())
                                                .build())
                                        .build())
                                .putFields("errorMessage", StringOptions.standard())
                                .putFields("eventType", StringOptions.strict())
                                .putFields("sourceIPAddress", StringOptions.strict())
                                .putFields("eventName", StringOptions.strict())
                                .putFields("eventSource", StringOptions.strict())
                                .putFields("recipientAccountId", LongOptions.lenient())
                                .putFields("awsRegion", StringOptions.strict())
                                .putFields("requestID", StringOptions.standard())
                                .putFields("eventVersion", StringOptions.strict())
                                .putFields("eventTime", InstantOptions.strict())
                                .build()))
                        .build())
                // .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail.json.nd"))
                // .multiValueSupport(true)
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail00.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail01.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail02.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail03.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail04.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail05.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail06.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail07.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail08.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail09.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail10.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail11.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail12.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail13.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail14.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail15.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail16.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail17.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail18.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail19.json"))
                .build());
    }
}
