/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.table.Table;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class WeatherStations {

    public static Table table() throws MalformedURLException {
        return JsonTableOptions.builder()
                .addSources(new URL("https://api.weather.gov/stations"))
                .options(api())
                .build()
                .execute();
    }

    public static Table tableLocal() {
        final List<Source> sources = new ArrayList<>();
        for (int i = 0; i < 91; ++i) {
            sources.add(Source.of(Path.of(String.format("/home/devin/Downloads/api.weather.gov/stations-%d.json", i))));
        }
        return JsonTableOptions.builder()
                .addAllSources(sources)
                .options(api())
                .build()
                .execute();
    }

    private static ObjectOptions api() {
        return ObjectOptions.builder()
                .putFields("features", ArrayOptions.strict(feature()))
                .build();
    }

    public static ObjectOptions feature() {
        return ObjectOptions.builder()
                .putFields("id", StringOptions.standard())
                .putFields("geometry", ObjectOptions.builder()
                        .putFields("coordinates", TupleOptions.builder()
                                .addValues(DoubleOptions.strict(), DoubleOptions.strict())
                                .build())
                        .build())
                .putFields("properties", properties())
                .build();
    }

    public static ObjectOptions properties() {
        return ObjectOptions.builder()
                .putFields("stationIdentifier", StringOptions.standard())
                .putFields("name", StringOptions.standard())
                .putFields("timeZone", StringOptions.standard())
                .putFields("forecast", StringOptions.standard())
                .putFields("county", StringOptions.standard())
                .putFields("fireWeatherZone", StringOptions.standard())
                .build();
    }
}
