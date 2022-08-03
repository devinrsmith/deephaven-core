package io.deephaven.web;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class WebLayout {

    public static Optional<WebLayout> systemPropertiesInstance() {
        final Optional<Path> path = layoutPathOrSearch();
        if (path.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new WebLayout(path.get()));
    }

    private static Optional<Path> layoutPathOrSearch() {
        final String layoutFile = System.getProperty("deephaven.web.layoutFile");
        if (layoutFile != null) {
            return Optional.of(Path.of(layoutFile));
        }
        for (Path path : layoutSearchPaths()) {
            if (Files.exists(path)) {
                return Optional.of(path);
            }
        }
        return Optional.empty();
    }

    private static List<Path> layoutSearchPaths() {
        // consider https://github.com/dirs-dev/directories-jvm
        return List.of(Path.of("layout.json"));
    }

    private final Path path;

    public WebLayout(Path path) {
        this.path = Objects.requireNonNull(path);
    }

    Path path() {
        return path;
    }
}
