use super::ClientNode;
use crate::{config::Config, lattice::Lattice, topics::RoutingThread};
use eyre::{anyhow, bail, Context};
use std::{
    future::Future,
    io::{BufRead, BufReader, Read, Write},
    time::Duration,
};

pub fn block_on<F: Future>(future: F) -> F::Output {
    log::warn!("`block_on` doesn't work in the current thread runtime, because the background TCP stream will be broken");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create async runtime")
        .block_on(future)
}

/// Starts a client node in [`interactive`][ClientNode::run_interactive] mode with the supplied config.
///
/// The given `stdin` handle is used for reading user input. This can be a handle to
/// [`std::io::Stdin`] to read from the console or an `&[u8]` slice to run a predefined set
/// of commands (e.g. for testing). The `stdout` handle is used for writing status messages
/// and `stderr` is used for logging errors.
///
/// The normal operation of the client is to ignore any invalid commands and only log an error
/// to `stderr`. This behavior can be changed by setting the `fail_fast` flag. If it is set,
/// this function will exit with an `Err` when it encounters invalid input, which is useful
/// for testing.
///
/// To communicate with routing and KVS nodes, the client uses the given zenoh workspace.
pub fn run_interactive<'a>(
    config: &Config,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    fail_fast: bool,
) -> eyre::Result<()> {
    let routing_threads: Vec<_> = (0..config.routing_threads)
        .map(|i| RoutingThread::new(i))
        .collect();

    let timeout = Duration::from_secs(10);
    let mut client = ClientNode::new(
        format!("client-{}", uuid::Uuid::new_v4()),
        0,
        routing_threads,
        timeout,
    )?;

    block_on(client.init_tcp_connections())?; // init TCP connections to routing nodes
    client.run_interactive(stdin, stdout, stderr, fail_fast)
}

impl ClientNode {
    /// Starts an interactive run loop that prompts the user for operations.
    ///
    /// Supported operations are:
    ///
    /// <table>                    <thead><tr><th>Operation</th><th>Effect</th></tr></thead><tbody>
    ///                                                                                    <tr><td>
    ///
    /// **`PUT <key> <value>`**                                                           </td><td>
    ///
    /// Writes the given `<key>` with the given `<value>` into the key value store. Uses the
    /// [`LastWriterWinsLattice`] type for values to achieve
    /// _"read commited"_ consistency.                                           </td></tr><tr><td>
    ///
    /// **`GET <key>`**                                                                   </td><td>
    ///
    /// Queries the value for the given `<key>` from the key value store. Expects that the value
    /// is of type [`LastWriterWinsLattice`], so it should be only used to read values that were
    /// written using `PUT`.                                                     </td></tr><tr><td>
    ///
    /// **`PUT_SET <key> <value1> <value2> ...`**                                         </td><td>
    ///
    /// Writes the given `<key>` with a the given set of values. Uses the [`SetLattice`] type for
    /// storing values, which resolves conflicts by taking the
    /// [union](https://en.wikipedia.org/wiki/Union_(set_theory)) of all conflicting
    /// sets.                                                                    </td></tr><tr><td>
    ///
    /// **`GET_SET <key>`**                                                               </td><td>
    ///
    /// Used to read a set of values that was previously written using
    /// `PUT_SET`.                                                               </td></tr><tr><td>
    ///
    /// **`PUT_CAUSAL <key> <value>`**                                                    </td><td>
    ///
    /// Writes the given `<key>` with the given `<value>` into the key value store. Uses the
    /// [`MultiKeyCausalLattice`] type to achieve
    /// [_"causal consistency"_](https://en.wikipedia.org/wiki/Causal_consistency) for the
    /// value.                                                                   </td></tr><tr><td>
    ///
    /// **`GET_CAUSAL <key>`**                                                            </td><td>
    ///
    /// Used to read a value with _"causal consistency"_ that was previously written using
    /// `PUT_CAUSAL`.                                                            </td></tr></tbody>
    ///
    /// </table>
    ///
    /// All operations can also be written in lowercase, e.g. `put_set` instead of `PUT_SET`.
    pub fn run_interactive(
        mut self,
        stdin: &mut dyn Read,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
        fail_fast: bool,
    ) -> eyre::Result<()> {
        let print_prompt = |stdout: &mut dyn Write| {
            write!(stdout, "kvs> ").context("failed to write to stdout")?;
            stdout.flush().context("failed to flush stdout")?;
            eyre::Result::<(), eyre::Error>::Ok(())
        };

        print_prompt(stdout)?;

        for line in BufReader::new(stdin).lines() {
            let input = line.context("failed to read line from stdin")?;
            match input.as_str().trim_end() {
                "quit" | "exit" | "q" => break,
                _ => {
                    if let Err(err) = self.handle_interactive_request(input, stdout) {
                        writeln!(stderr, "Error: {:#}", err)?;
                        if fail_fast {
                            bail!(err);
                        }
                    }
                }
            }
            print_prompt(stdout)?;
        }
        Ok(())
    }

    fn handle_interactive_request(
        &mut self,
        input: String,
        stdout: &mut dyn Write,
    ) -> eyre::Result<()> {
        const HELP: &str = "\n\nValid commands are are GET, GET_SET, PUT, PUT_SET, \
            PUT_CAUSAL, and GET_CAUSAL.";

        let mut split = input.split_whitespace();
        let command = split
            .next()
            .ok_or_else(|| anyhow!("no command entered.{}", HELP))?;
        match command {
            "PUT" | "put" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key and value arguments"))?;
                let value = split
                    .next()
                    .ok_or_else(|| anyhow!("missing value argument"))?;
                if let Some(extra) = split.next() {
                    bail!("unexpected argument `{}`", extra);
                }

                block_on(self.put_lww(key.into(), value.into()))?;
                writeln!(stdout, "Success!")?;
            }
            "GET" | "get" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key argument"))?;
                if let Some(extra) = split.next() {
                    bail!("unexpected argument `{}`", extra);
                }

                let value = block_on(self.get_lww(key.into()))?;
                let string = String::from_utf8(value).context("value is not valid utf8")?;

                log::trace!("[OK] Got {} from GET", string);
                writeln!(stdout, "{}", string)?;
            }
            "PUT_SET" | "put_set" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key and value arguments"))?;
                let value = split.map(|s| s.to_owned().into_bytes()).collect();

                block_on(self.put_set(key.into(), value))?;
                writeln!(stdout, "Success!")?;
            }
            "GET_SET" | "get_set" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key argument"))?;
                if let Some(extra) = split.next() {
                    bail!("unexpected argument `{}`", extra);
                }

                let value = block_on(self.get_set(key.into()))?;

                let mut set: Vec<_> = value
                    .iter()
                    .map(|v| std::str::from_utf8(v))
                    .collect::<Result<_, _>>()
                    .context("received value is not valid utf8")?;
                set.sort_unstable();

                log::trace!("[OK] Got {:?} from GET_SET", set);

                writeln!(stdout, "{{ {} }}", set.join(" "))?;
            }
            "PUT_CAUSAL" | "put_causal" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key and value arguments"))?;
                let value = split
                    .next()
                    .ok_or_else(|| anyhow!("missing value argument"))?;
                if let Some(extra) = split.next() {
                    bail!("unexpected argument `{}`", extra);
                }

                block_on(self.put_causal(key.into(), value.to_owned().into_bytes()))?;
                writeln!(stdout, "Success!")?;
            }
            "GET_CAUSAL" | "get_causal" => {
                let key = split
                    .next()
                    .ok_or_else(|| anyhow!("missing key argument"))?;
                if let Some(extra) = split.next() {
                    bail!("unexpected argument `{}`", extra);
                }

                let mkcl = block_on(self.get_causal(key.into()))?;

                for (k, v) in mkcl.vector_clock.reveal() {
                    writeln!(stdout, "{{{} : {}}}", k, v.reveal())?;
                }

                for (k, v) in mkcl.dependencies.reveal() {
                    write!(stdout, "{} : ", k)?;
                    for vc_pair in v.reveal() {
                        writeln!(stdout, "{{{} : {}}}", vc_pair.0, vc_pair.1.reveal())?;
                    }
                }

                let values = mkcl
                    .value
                    .reveal()
                    .iter()
                    .map(|v| String::from_utf8(v.to_owned()))
                    .collect::<Result<Vec<_>, _>>()?;

                assert_eq!(values.len(), 1);

                writeln!(stdout, "{}", values[0])?;
            }
            other => bail!("unrecognized command `{}`.{}", other, HELP),
        }
        Ok(())
    }
}
