/**
 * This package translates {@link io.deephaven.api.expression.Expression expressions},
 * {@link io.deephaven.api.filter.Filter filters}, and {@link io.deephaven.api.literal.Literal literals} into
 * <i>best-effort</i> "engine strings"; strings that <b>may</b> be compilable by the engine at runtime. In cases where
 * there is no such plausible engine string, an {@link io.deephaven.engine.table.impl.strings.UnsupportedEngineString}
 * will be thrown. For example, {@link io.deephaven.api.filter.FilterPattern} has no engine string.
 *
 * <p>
 * The engine currently uses engine strings as a fallback mechanism for when more explicitly constructed implementations
 * don't exist. These cases represent potential places for engine optimizations in the future.
 *
 * <p>
 * The java client currently uses engine strings in cases where more explicitly typed RPCs don't exist. These cases
 * represent potential places for RPC additions in the future.
 */
package io.deephaven.engine.table.impl.strings;
