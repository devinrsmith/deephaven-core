package io.deephaven.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
@JsonSerialize(as = ImmutableWebConfig.class)
@JsonDeserialize(as = ImmutableWebConfig.class)
public abstract class WebConfig {

    public static Builder builder() {
        return ImmutableWebConfig.builder();
    }

    public static WebConfig systemPropertiesInstance() {
        final String csvDownload = System.getProperty("deephaven.web.csvDownload");
        final String copy = System.getProperty("deephaven.web.copy");
        final String console = System.getProperty("deephaven.console.disable");
        final Builder builder = builder();
        if (csvDownload != null) {
            builder.csvDownload(Boolean.parseBoolean(csvDownload));
        }
        if (copy != null) {
            builder.copy(Boolean.parseBoolean(copy));
        }
        if (console != null) {
            builder.console(Boolean.parseBoolean(console));
        }
        return builder.build();
    }

    // https://github.com/deephaven/web-client-ui/issues/703
    @Default
    @JsonProperty("csv_download")
    public boolean csvDownload() {
        return true;
    }

    // https://github.com/deephaven/web-client-ui/issues/703
    @Default
    @JsonProperty("copy")
    public boolean copy() {
        return true;
    }

    // https://github.com/deephaven/web-client-ui/issues/596
    @Default
    @JsonProperty("console")
    public boolean console() {
        return true;
    }

    public interface Builder {

        Builder csvDownload(boolean csvDownload);

        Builder copy(boolean copy);

        Builder console(boolean console);

        WebConfig build();
    }
}
