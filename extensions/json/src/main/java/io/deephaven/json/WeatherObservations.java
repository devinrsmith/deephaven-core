/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.table.Table;

import java.net.MalformedURLException;
import java.net.URL;

public class WeatherObservations {

    public static Table table() throws MalformedURLException {
        return JsonTableOptions.builder()
                .addSources(new URL("https://api.weather.gov/stations/KNYC/observations"))
                .addSources(new URL("https://api.weather.gov/stations/KCOS/observations"))
                .options(api())
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
                .putFields("@id", StringOptions.strict())
                .putFields("@type", StringOptions.strict())
                .putFields("elevation", measurementInt())
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
                .putFields("barometricPressure", measurementInt())
                .putFields("seaLevelPressure", measurementInt())
                .putFields("visibility", measurementInt())
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
                .putFields("unitCode", StringOptions.strict())
                .putFields("value", IntOptions.standard())
                .putFields("qualityControl", StringOptions.standard())
                .build();
    }

    private static ObjectOptions measurement() {
        return ObjectOptions.builder()
                .putFields("unitCode", StringOptions.strict())
                .putFields("value", DoubleOptions.standard())
                .putFields("qualityControl", StringOptions.standard())
                .build();
    }
}
