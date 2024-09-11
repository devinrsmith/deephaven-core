- advanced ingest
    - focus on composabiliity
        - lacks bulk "fill"; optimized for row-oriented "streaming" formats like protobuf, avro, JSON
        - column-oriented writers need to do continual appends
    - "state-machine" like drivers
    - Multiple users, use-cases:
        - async streaming event sources (kafka) -> Blink
        - manual driven sources (user execute code) -> Blink
        - DIS
        - "static" event sources, like JSONL file
            - could be thought of as "streaming source", but with clear ending
            - should be capable of producing intermediate results without needing to complete whole file
            - For example, may want to produce an aggregration on a very large JSONL file, don't want to need to pull it all into memory at once
    - TODO: mark a source as "done"
    - For event driver architectures:
        - 1 event in, multiple rows out (including 0)
        - 1 event in, multiple "tables" out
        - 1 event in, multiple "tables" with multiple rows out
        - There is *not* cross-event synchronization
        - Possible that advanced use cases may "build it themselves" since writing interface is wide open
    - Support for column-oriented, row-oriented, or "mix" oriented writing patterns
        - Explicitly not aiming to support random-write patterns
            - Write for a given column proceed in an "append-only" fashion
        - Explicitly not aiming to support a "previous" or "backup" or "undo" operation
        - In row oriented mode, if a row is *not* advanced, there is explicit support for *re-writing* a column.
            - This is necessary to support protobuf, https://protobuf.dev/programming-guides/encoding/#last-one-wins
            - Allows JSON to support "last-key" "efficiently":
              -
                ```json
                {
                  "foo": 42,
                  "foo": 43
                }
                ```
            - Implementations may disallow re-writes (for example, it should not be possible w/ avro)
        - advanceAll optimizations for row-oriented writers
    - For multiple blink tables out
        - Will need some higher level coordinator besides "flush" (only exists for 1 table)