package io.deephaven.chunk;

import java.util.List;
import java.util.function.Consumer;

public class ChunksToStream {


    class KeyConsumer implements Consumer<List<? extends WritableChunks>> {
        @Override
        public void accept(List<? extends WritableChunks> writableChunks) {

        }
    }

    class ValueConsumer implements Consumer<List<? extends WritableChunks>> {
        @Override
        public void accept(List<? extends WritableChunks> writableChunks) {

            // todo: how do we associate kafka offset w/ multiple output rows?
            // and, what about representiing empty output row?
        }
    }
}
