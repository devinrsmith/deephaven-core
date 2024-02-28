/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.table.Table;
import io.deephaven.function.Random;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Perf {

    public static Table table(int maxThreads, int numMessages, int chunkSize) {
        final List<Source> sources = Arrays.stream(generateBigMessages(numMessages)).map(Source::of).collect(Collectors.toList());
        final ObjectOptions opts = bigMessageOptions();
        return JsonTableOptions.builder()
                .options(opts)
                .addAllSources(sources)
                .maxThreads(maxThreads)
                .chunkSize(chunkSize)
                .build()
                .execute();
    }

    private static ObjectOptions bigMessageOptions() {
        final Map<String, ValueOptions> fields = new LinkedHashMap<>();
        for (int i = 0; i < 30; ++i) {
            fields.put(String.format("str%d", i), StringOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            fields.put(String.format("dbl%d", i), DoubleOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            fields.put(String.format("iger%d", i), IntOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            // todo short
            fields.put(String.format("shrt%d", i), IntOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            fields.put(String.format("lng%d", i), LongOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            // todo byte
            fields.put(String.format("byt%d", i), IntOptions.strict());
        }
        for (int i = 0; i < 30; ++i) {
            fields.put(String.format("flt%d", i), FloatOptions.strict());
        }
        return ObjectOptions.builder().putAllFields(fields).build();
    }


    public static String[] generateBigMessages(int numMessages) {
        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatCol = "flt";

        final String[] names = new String[140];
        @SuppressWarnings("rawtypes") final Class[] types = new Class[names.length];
        final int groupSize = names.length / 7;
        for (int i = 0; i < names.length; i++) {
            if (i < groupSize) {
                names[i] = strCol + (i % groupSize);
                types[i] = String.class;
            } else if (i < 2 * groupSize) {
                names[i] = dblCol + (i % groupSize);
                types[i] = double.class;
            } else if (i < 3 * groupSize) {
                names[i] = intCol + (i % groupSize);
                types[i] = int.class;
            } else if (i < 4 * groupSize) {
                names[i] = shortCol + (i % groupSize);
                types[i] = short.class;
            } else if (i < 5 * groupSize) {
                names[i] = longCol + (i % groupSize);
                types[i] = long.class;
            } else if (i < 6 * groupSize) {
                names[i] = byteCol + (i % groupSize);
                types[i] = byte.class;
            } else {
                names[i] = floatCol + (i % groupSize);
                types[i] = float.class;
            }
        }
        final String[] messages = new String[numMessages];
        System.out.println("Generating test messages...");
        final int mgGroupSize = 30;
        final StringBuilder builder = new StringBuilder(1024);
        for (int i = 0; i < messages.length; i++) {
            builder.setLength(0);
            builder.append('{').append(System.lineSeparator());
            for (int mg = 0; mg < 210; mg++) {
                if (mg < mgGroupSize) {
                    builder.append("\"" + strCol).append(mg % mgGroupSize).append("\": \"test").append(i).append("\",")
                            .append(System.lineSeparator());
                } else if (mg < 2 * mgGroupSize) {
                    builder.append("\"" + dblCol).append(mg % mgGroupSize).append("\": ").append(Random.random())
                            .append(',')
                            .append(System.lineSeparator());
                } else if (mg < 3 * mgGroupSize) {
                    builder.append("\"" + intCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomInt(0, Integer.MAX_VALUE)).append(',')
                            .append(System.lineSeparator());
                } else if (mg < 4 * mgGroupSize) {
                    builder.append("\"" + shortCol).append(mg % mgGroupSize).append("\": ").append(mg + 1).append(',')
                            .append(System.lineSeparator());
                } else if (mg < 5 * mgGroupSize) {
                    builder.append("\"" + longCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomLong(-Long.MAX_VALUE, Long.MAX_VALUE))
                            .append(',').append(System.lineSeparator());
                } else if (mg < 6 * mgGroupSize) {
                    builder.append("\"" + byteCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1)).append(',')
                            .append(System.lineSeparator());
                } else {
                    builder.append("\"" + floatCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomFloat(-10000, 10000)).append(',')
                            .append(System.lineSeparator());
                }
            }
            builder.deleteCharAt(builder.length() - 2); // Remove trailing comma
            builder.append('}');
            messages[i] = builder.toString();
        }
        return messages;
    }
}
