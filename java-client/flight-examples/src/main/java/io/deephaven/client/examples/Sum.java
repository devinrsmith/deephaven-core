package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collections;

@Command(name = "sum", mixinStandardHelpOptions = true,
        description = "Sum up to the count and print out the results",
        version = "0.1.0")
class Sum extends FlightCannedTableBase implements TableCreationLogic {

    @Option(names = {"-t", "--target"}, description = "The host target, default: ${DEFAULT-VALUE}",
            defaultValue = "localhost:10000")
    String target;

    @Option(names = {"-c", "--count"}, description = "The number of sums, defaults to 100000000",
            defaultValue = "100000000")
    long count;

    @Override
    protected String target() {
        return target;
    }

    @Override
    protected TableCreationLogic logic() {
        return this;
    }

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        return c.of(TableSpec.empty(count))
                .view("I=i")
                .by(Collections.emptyList(), Collections.singleton(io.deephaven.api.agg.Sum.of("Sum=I")));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Sum()).execute(args);
        System.exit(execute);
    }
}
