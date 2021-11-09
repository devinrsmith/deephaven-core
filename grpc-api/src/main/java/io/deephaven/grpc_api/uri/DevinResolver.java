package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.LongArray.Builder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;

import javax.inject.Inject;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DevinResolver implements UriResolver {
    public static final Set<String> SCHEMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("dh", "dh+plain")));

    public static final String EXPECTED_PATH = "/devin";

    public static final Pattern QUERY_PATTERN = Pattern.compile("^\\?foo=(.+)&bar=(.+)$");

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
        final String fooGroup = matcher.group(1);
        final String barGroup = matcher.group(2);
        return fooBar(fooGroup, barGroup);
    }

    public static Table fooBar(String fooGroup, String barGroup) {
        final TableSpec foo = NewTable.of(Column.of("Foo", toArray(fooGroup)));
        final TableSpec bar = NewTable.of(Column.of("Bar", toArray(barGroup)));
        final TableSpec results = foo
                .join(bar, "")
                .updateView("Add=Foo+Bar", "Mult=Foo*Bar");
        return Table.of(results);
    }

    @Override
    public Object resolveSafely(URI uri) throws InterruptedException {
        return resolve(uri);
    }

    private static LongArray toArray(String group) {
        final String[] foos = group.split(",");
        final Builder fooBuilder = LongArray.builder(foos.length);
        for (String foo : foos) {
            if (foo.isEmpty()) {
                fooBuilder.add((Long) null);
            } else {
                fooBuilder.add(Long.parseLong(foo));
            }
        }
        return fooBuilder.build();
    }
}
