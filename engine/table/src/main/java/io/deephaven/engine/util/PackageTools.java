/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.table.Table;

import java.util.stream.Stream;

import static io.deephaven.engine.util.TableTools.booleanCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;

public final class PackageTools {

    public static Table packages() {
        return table(Package.getPackages());
    }

    public static Table packages(ClassLoader classLoader) {
        return table(classLoader.getDefinedPackages());
    }

    private static Table table(Package[] packages) {
        return newTable(
                stringCol("Name", Stream.of(packages).map(Package::getName).toArray(String[]::new)),
                stringCol("SpecificationTitle",
                        Stream.of(packages).map(Package::getSpecificationTitle).toArray(String[]::new)),
                stringCol("SpecificationVersion",
                        Stream.of(packages).map(Package::getSpecificationVersion).toArray(String[]::new)),
                stringCol("SpecificationVendor",
                        Stream.of(packages).map(Package::getSpecificationVendor).toArray(String[]::new)),
                stringCol("ImplementationTitle",
                        Stream.of(packages).map(Package::getImplementationTitle).toArray(String[]::new)),
                stringCol("ImplementationVersion",
                        Stream.of(packages).map(Package::getImplementationVersion).toArray(String[]::new)),
                stringCol("ImplementationVendor",
                        Stream.of(packages).map(Package::getImplementationVendor).toArray(String[]::new)),
                booleanCol("Sealed", Stream.of(packages).map(Package::isSealed).toArray(Boolean[]::new)));
    }
}
