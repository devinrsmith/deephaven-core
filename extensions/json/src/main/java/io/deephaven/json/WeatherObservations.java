/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WeatherObservations {

    public static Table table(String stationId) throws MalformedURLException {
        return JsonTableOptions.builder()
                .addSources(new URL(String.format("https://api.weather.gov/stations/%s/observations", stationId)))
                .options(api())
                .maxThreads(1)
                .build()
                .execute();
    }

    public static Table table(Table ids) {
        final List<Source> sources = new ArrayList<>();
        final TrackingRowSet rowSet = ids.getRowSet();
        final Iterator<? extends ColumnSource<?>> it = ids.getColumnSources().iterator();
        if (!it.hasNext()) {
            throw new IllegalArgumentException();
        }
        final ColumnSource<?> source = it.next();
        if (it.hasNext()) {
            throw new IllegalArgumentException();
        }
        rowSet.forAllRowKeys(rowKey -> {
            try {
                sources.add(Source.of(new URL(String.format("https://api.weather.gov/stations/%s/observations", source.get(rowKey)))));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        });
        return JsonTableOptions.builder()
                .addAllSources(sources)
                .options(api())
                .maxThreads(1)
                .build()
                .execute();
    }

    private static ObjectOptions api() {
        return ObjectOptions.builder()
                .putFields("features", ArrayOptions.strict(feature()))
                .build();
    }

    private static ObjectOptions feature() {
        return ObjectOptions.builder()
                .putFields("id", StringOptions.strict())
                .putFields("type", StringOptions.strict())
                .putFields("properties", properties())
                .build();
    }

    private static ObjectOptions properties() {
        return ObjectOptions.builder()
                .putFields("elevation", measurement())
                .putFields("station", StringOptions.strict())
                .putFields("timestamp", InstantOptions.strict())
                .putFields("rawMessage", StringOptions.strict())
                .putFields("textDescription", StringOptions.strict())
                .putFields("icon", StringOptions.standard())
                .putFields("temperature", measurement())
                .putFields("dewpoint", measurement())
                .putFields("windDirection", measurement())
                .putFields("windSpeed", measurement())
                .putFields("windGust", measurement())
                .putFields("barometricPressure", measurement())
                .putFields("seaLevelPressure", measurement())
                .putFields("visibility", measurement())
                .putFields("maxTemperatureLast24Hours", measurement())
                .putFields("minTemperatureLast24Hours", measurement())
                .putFields("precipitationLastHour", measurement())
                .putFields("precipitationLast3Hours", measurement())
                .putFields("precipitationLast6Hours", measurement())
                .putFields("relativeHumidity", measurement())
                .putFields("windChill", measurement())
                .putFields("heatIndex", measurement())
                .build();
    }

    private static ObjectOptions measurementInt() {
        return ObjectOptions.builder()
                .putFields("unitCode", StringOptions.standard())
                .putFields("value", IntOptions.standard())
                .putFields("qualityControl", StringOptions.standard())
                .build();
    }

    private static ObjectOptions measurement() {
        return ObjectOptions.builder()
                .putFields("unitCode", StringOptions.standard())
                .putFields("value", DoubleOptions.standard())
                .putFields("qualityControl", StringOptions.standard())
                .build();
    }
}
