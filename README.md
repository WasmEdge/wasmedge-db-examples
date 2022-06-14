# anna-rs

Rust port of the **[`anna`](https://github.com/hydro-project/anna)** key-value store, using **[`zenoh`](https://zenoh.io/)** for communication.

**Note:** This project is still in a prototype state, so don't use it in production!

## Attribution

This crate started out as a line-by-line port of the original **[`anna`](https://github.com/hydro-project/anna)** project developed in the [RISE Lab](https://rise.cs.berkeley.edu) at [UC Berkeley](https://berkeley.edu). We recommend to check out their [ICDE 2018 paper](http://db.cs.berkeley.edu/jmh/papers/anna_ieee18.pdf) for more information.

## Build

You need the latest version of [Rust](https://www.rust-lang.org/) for building. The build commands are `cargo build` for a debug build and `cargo build --release` for an optimized release build. After building, you can find the resulting executables under `../target/debug` (for debug builds) or `../target/release` (for release builds).

To run the **test suite**, execute `cargo test`. The **API documentation** can be generated through `cargo doc --open`.

## Run

This project contains several executables:

- **`kvs`**: The main key-value store node.
- **`routing`**: Routing nodes are responsible for coordinating kvs nodes and keeping track of which key is mapped to which node.
- **`client`**: A client proxy that provides an interactive prompt where you can send `GET`/`PUT` commands.
- **`logger`**: Subscribes to all local `zenoh` messages and logs them to stdout.
- **`benchmark`**: The benchmark node provides different ways to evaluate the performance of the key-value store. To trigger benchmark commands, the `send-benchmark-commands` executable can be used.
- TODO: monitoring, management

Each executable can be started by running the built executable from `target`. Alternatively, it's also possible to combine the build and run steps using `cargo run --bin <bin-name>`, where `<bin-name>` is the name of the executable. Since all nodes expect that an routing node is available, it is recommended to start the `routing` executable first.

### Config Files

All executables (with the exception of the logger) expect the path to a config file as argument. An example for this config file can be found in [`example-config.yml`](example-config.yml).

With `cargo run`, this argument can be passed in the following way:

```
cargo run --bin <bin-name> -- <path>
```

Note the additional space between `--` and `<path>`. The `cargo run` command uses a `--` argument as separator, so everything after it is passed to the executable.

### Example

Open four terminal windows and run the following commands in them (one per terminal window):

1. `cargo run --bin logger` to start the zenoh logger, so that we can see the messages that are sent. This step is optional.
2. `cargo run --bin routing -- example-config.yml` to start the routing node.
3. `cargo run --bin kvs -- example-config.yml` to start the key-value store node.
4. `cargo run --bin client -- example-config.yml` to start the client proxy.

The client proxy executable will show a `kvs>` prompt, in which you can use the following commands:

<!-- Note: Keep this in sync with the `run_interactive` docs in src/nodes/client/mod.rs -->

<table>                    <thead><tr><th>Operation</th><th>Effect</th></tr></thead><tbody>
                                                                                    <tr><td>

**`PUT <key> <value>`**                                                           </td><td>

Writes the given `<key>` with the given `<value>` into the key value store. Uses the
`LastWriterWinsLattice` type for values to achieve
_"read commited"_ consistency.                                           </td></tr><tr><td>

**`GET <key>`**                                                                   </td><td>

Queries the value for the given `<key>` from the key value store. Expects that the value
is of type `LastWriterWinsLattice`, so it should be only used to read values that were
written using `PUT`.                                                     </td></tr><tr><td>

**`PUT_SET <key> <value1> <value2> ...`**                                         </td><td>

Writes the given `<key>` with a the given set of values. Uses the `SetLattice` type for
storing values, which resolves conflicts by taking the
[union](https://en.wikipedia.org/wiki/Union_(set_theory)) of all conflicting
sets.                                                                    </td></tr><tr><td>

**`GET_SET <key>`**                                                               </td><td>

Used to read a set of values that was previously written using
`PUT_SET`.                                                               </td></tr><tr><td>

**`PUT_CAUSAL <key> <value>`**                                                    </td><td>

Writes the given `<key>` with the given `<value>` into the key value store. Uses the
`MultiKeyCausalLattice` type to achieve
[_"causal consistency"_](https://en.wikipedia.org/wiki/Causal_consistency) for the
value.                                                                   </td></tr><tr><td>

**`GET_CAUSAL <key>`**                                                            </td><td>

Used to read a value with _"causal consistency"_ that was previously written using
`PUT_CAUSAL`.                                                            </td></tr></tbody>

</table>

All operations can also be written in lowercase, e.g. `put_set` instead of `PUT_SET`.

For a description of the mentioned lattice types (e.g. `LastWriterWinsLattice`), see the `lattice` module in the API docs of this crate, which you can generate using **`cargo doc --open`**.

## Benchmarks

For benchmarking, the `benchmark` executable can be used. It spawns a proxy node that listens for incoming benchmark commands and executes them. It supports a variety of different commands, for example for executing a specific number of random `PUT` requests. To send commands, the `send-benchmark-commands` executable can be used.

### Example

Run the `routing` and `kvs` executables as described above under [_Example_](#example), but in `--release` mode. Then open two additional terminal windows. In the first, spawn the benchmark node by running `cargo run --bin benchmark --release -- example-config.yml`. From the second window, run `cargo run --bin send-benchmark-commands -- <command>` to send trigger the benchmark command `<command>`.

### Commands

The following commands are supported:

- **`cache <num_keys>`:** Determines the responsible KVS nodes for keys `0..num_keys` and caches this information for subsequent commands.
- **`warm <num_keys> <value_len> <total_threads>`:** Generates `num_keys` `PUT` requests to the key-value store. The `total_threads` argument must be the configured number of benchmark threads (the requests are split between the threads). The stored values are of the form `aaaaaaa...`, with a length determined by the `value_len` argument.
- **`load <ty> <num_keys> <value_len> <report_period> <time> <zipf>`:** Performs a random set of operations with the given `zipf` coefficient. If `zipf` is 0, a uniform random distribution is used. The `time` argument specifies how long the benchmark should run (in seconds) and the `report_period` specifies the interval (in seconds) of throughput reports.

  The `num_keys` argument specifies the set of possible keys: the random keys will be selected from the range `0..num_keys`. The `value_len` parameter specifies the number of `a` characters in the stored values (all values are of the form `aaaaaaaa...`).

  The `ty` parameter specifies what kind of operations should be performed on the random set of keys. The following options are available:

    - **`G`:** Performs `GET` requests.
    - **`P`:** Performs `PUT` requests.
    - **`M`:** Perfomrs a `PUT` request and then a `GET` request on the same key; also reports the latency of such a `PUT`/`GET` pair.

The results are reported via zenoh messages. A subset of results is also reported as log messages in the `stdout` of the `benchmark` executable (not the `send-benchmark-commands` executable) and in the `benchmark-thread-<thread_num>.log` file.

Make sure to compile all nodes in `--release` mode when benchmarking!

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>). Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.
