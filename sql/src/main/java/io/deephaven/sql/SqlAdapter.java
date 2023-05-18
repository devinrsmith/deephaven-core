/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * This is the main public entrypoint for converting a SQL query into {@link TableSpec}.
 */
public final class SqlAdapter {

    private static final Logger log = LoggerFactory.getLogger(SqlAdapter.class);

    /**
     * Parses the {@code sql} query into a {@link TableSpec}.
     *
     * @param sql the sql
     * @param headers the headers
     * @return the table spec
     */
    public static TableSpec parseSql(
            String sql,
            Map<List<String>, TableHeader> headers) {
        // 0: Configuration
        // SQLTODO(parse-sql-configuration)
        //
        // Allow for customization of calcite parsing details. Would this mean that calcite should / would become part
        // of the public API, or would it be kept as an implementation detail?

        // 1: Parse into AST
        final SqlNode node = parse(sql);

        // 2: Validate AST
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final CatalogReader catalogReader = reader(typeFactory, headers);
        final SqlValidator validator = validator(typeFactory, catalogReader);
        final SqlNode validNode = validator.validate(node);

        // 3: Convert into relational node
        final RelNode relNode = convert(typeFactory, catalogReader, validator, validNode);
        log.debug(
                RelOptUtil.dumpPlan("[Logical plan]", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        // 4: Relational optimization
        // SQLTODO(rel-node-optimization)
        //
        // Use calcite for optimization. Simple optimizations are filter / predicate pushdowns (for example, any filters
        // exclusively on the LHS or RHS of a join). More advanced optimizations may be possible by attaching statistics
        // or hints about the underlying structure of the source data.

        // 5. Convert into QST
        // SQLTODO(qst-convert-optimization)
        //
        // The conversion process from RelNode to QST can be optimized if additional hints are added to the source data,
        // or additional relational details about the data are known.
        //
        // For example, if we know that all Ids in a column are unique, either because the user has hinted as much, or
        // we know the Ids are unique because they are a "last_by" construction, we may be able to use a natural_join
        // instead of a join for an INNER JOIN conversion.
        return RelNodeAdapterNamed.of(SqlRootContext.of(relNode), relNode);

        // 6. QST optimization
        // SQLTODO(qst-optimization)
        // There are QST optimizations that are orthogonal to SQL, but none-the-less may be useful and can be guided
        // based on the types of unoptimized structures that this implementation may make.
    }

    // SQLTODO(catalog-reader-implementation)
    //
    // The current interface assumes that the user will pass in all the headers they care about. When executing a SQL
    // query, this means that either a) the user is explicit about the context they care about or b) all possible
    // headers are collected. a) is not very user friendly and b) is inefficient.
    //
    // A potentially better model would be an interface where the caller could produce the TableHeaders on demand
    // instead of requiring them all up-front. This would require implementing / wrapping
    // org.apache.calcite.prepare.Prepare.CatalogReader which is possible, but non-trivial. There may also be
    // boundary-crossing concerns wrt python.
    //
    // public interface SchemaLookup {
    // Optional<TableHeader> get(List<String> qualifiedName);
    // }
    //
    // public static TableSpec parseSql(String sql, SchemaLookup schemaLookup) {
    // throw new UnsupportedOperationException();
    // }

    private static SqlNode parse(String sql) {
        final SqlParser.Config config = SqlParser.config();
        final SqlParser parser = SqlParser.create(sql, config);
        final SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw new SqlParseException(e);
        }
        return sqlNode;
    }

    private static CatalogReader reader(
            RelDataTypeFactory typeFactory,
            Map<List<String>, TableHeader> headers) {
        final CalciteSchema schema = CalciteSchema.createRootSchema(true);
        for (Entry<List<String>, TableHeader> e : headers.entrySet()) {
            if (e.getKey().size() != 1) {
                throw new UnsupportedOperationException("todo");
            }
            schema.add(e.getKey().get(0), new DeephavenTable(TypeAdapter.of(e.getValue(), typeFactory)));
        }
        final Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        final CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        // todo: what is bs?
        return new CalciteCatalogReader(schema, Collections.singletonList("bs"), typeFactory, config);
    }

    private static SqlValidator validator(
            RelDataTypeFactory typeFactory,
            CatalogReader catalogReader) {
        // add documentation about why DH does this
        final SqlValidator.Config config = SqlValidator.Config.DEFAULT.withDefaultNullCollation(NullCollation.LOW);
        return SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, config);
    }

    private static RelNode convert(
            RelDataTypeFactory typeFactory,
            CatalogReader catalogReader,
            SqlValidator validator,
            SqlNode validNode) {
        final RelOptCluster cluster = newCluster(typeFactory);
        final SqlToRelConverter converter = new SqlToRelConverter(NoopExpander.INSTANCE, validator, catalogReader,
                cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());
        return converter.convertQuery(validNode, false, true).rel;
    }

    enum NoopExpander implements ViewExpander {
        INSTANCE;

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                @Nullable List<String> viewPath) {
            return null;
        }
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }
}
