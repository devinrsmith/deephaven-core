package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;

import javax.inject.Inject;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DevinResolver implements UriResolver {
    public static final Set<String> SCHEMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("dh", "dh+plain")));

    public static final String EXPECTED_PATH = "/devin";

    public static final Pattern QUERY_PATTERN = Pattern.compile("^foo=(.+)&bar=(.+)$");

    @Inject
    public DevinResolver() {
    }

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        return uri.getHost() == null
                && !uri.isOpaque()
                && EXPECTED_PATH.equals(uri.getPath())
                && uri.getQuery() != null
                && QUERY_PATTERN.matcher(uri.getQuery()).matches()
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    @Override
    public Object resolve(URI uri) throws InterruptedException {
        if (!EXPECTED_PATH.equals(uri.getPath())) {
            throw new IllegalStateException();
        }
        final Matcher matcher = QUERY_PATTERN.matcher(uri.getQuery());
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        final List<Integer> fooGroup = Stream.of(matcher.group(1).split(",")).map(DevinResolver::map).collect(Collectors.toList());
        final List<Integer> barGroup = Stream.of(matcher.group(2).split(",")).map(DevinResolver::map).collect(Collectors.toList());
        return fooBar(fooGroup, barGroup);
    }

    public static Table fooBar(Collection<Integer> fooGroup, Collection<Integer> barGroup) {
        final TableSpec foo = NewTable.of(Column.of("Foo", IntArray.of(fooGroup)));
        final TableSpec bar = NewTable.of(Column.of("Bar", IntArray.of(barGroup)));
        final TableSpec results = foo
                .join(bar, "")
                .updateView("Add=Foo+Bar", "Mult=Foo*Bar");
        return Table.of(results);
    }

    private static Integer map(String input) {
        return input == null || input.isEmpty() ? null : Integer.parseInt(input);
    }

    @Override
    public Object resolveSafely(URI uri) throws InterruptedException {
        return resolve(uri);
    }
}
