package io.deephaven.plugin.app;

import java.util.Objects;

/**
 * State represents the collection or logical construction of named objects as part of an {@link App}.
 *
 * @see Consumer#set(String, Object)
 * @see Consumer#set(String, Object, String)
 */
public interface State {

    /**
     * Consume
     *
     * @param consumer the consumer
     */
    void insertInto(Consumer consumer);

    interface Consumer {

        String separator();

        /**
         * Creates a prefixed consumer, whose set methods are the equivalent of
         * {@code this.set(prefix + separator() + name, object)} or
         * {@code this.set(prefix + separator() + name, object, description)}.
         *
         * @param prefix the prefix
         * @return a prefixed consumer
         */
        Consumer prefix(String prefix);

        /**
         * A helper method equivalent to {@code prefix(prefix).set(state)}.
         * 
         * @param prefix the partial prefix
         * @param state the state
         */
        void set(String prefix, State state);

        /**
         * A helper method equivalent to {@code state.insertInto(this)}.
         *
         * @param state the state
         */
        void set(State state);

        /**
         * Set
         * 
         * @param name the name
         * @param object the object
         */
        void set(String name, Object object);

        /**
         *
         * @param name the name
         * @param object the object
         * @param description the description
         */
        void set(String name, Object object, String description);
    }

    /**
     * The base implementation of {@link Consumer}.
     */
    abstract class ConsumerBase implements Consumer {

        private final String separator;

        public ConsumerBase() {
            this(".");
        }

        public ConsumerBase(String separator) {
            this.separator = Objects.requireNonNull(separator);
        }

        @Override
        public final String separator() {
            return separator;
        }

        @Override
        public final Consumer prefix(String prefix) {
            return new PrefixedConsumer(prefix, this);
        }

        @Override
        public final void set(String prefix, State state) {
            prefix(prefix).set(state);
        }

        @Override
        public final void set(State state) {
            state.insertInto(this);
        }

        private static final class PrefixedConsumer extends ConsumerBase {
            private final String prefix;
            private final Consumer delegate;

            public PrefixedConsumer(String prefix, Consumer delegate) {
                super(delegate.separator());
                this.prefix = Objects.requireNonNull(prefix);
                this.delegate = Objects.requireNonNull(delegate);
            }

            @Override
            public void set(String name, Object object) {
                delegate.set(prefix + delegate.separator() + name, object);
            }

            @Override
            public void set(String name, Object object, String description) {
                delegate.set(prefix + delegate.separator() + name, object, description);
            }
        }
    }
}
