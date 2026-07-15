# win-ipc

shared-memory IPC channels for latency-sensitive services: [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) zero-copy transport + [wincode](https://crates.io/crates/wincode) serialization.

## Properties

- **Low latency**: messages are wincode-serialized directly into the loaned shm sample (single copy, no allocation); the receiver busy-polls its own thread, pinned to a required dedicated core.
- **Never blocks the sender**: safe-overflow queues drop oldest when a subscriber stalls.

## Example

Shared message type:

```rust
use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead, Debug)]
struct Msg {
    seq: u64,
    payload: Vec<u8>,
}
```

Producer process:

```rust
use win_ipc::{IpcConfig, IpcSender};

fn main() -> eyre::Result<()> {
    let sender = IpcSender::<Msg>::new("my/channel", &IpcConfig::default())?;
    for seq in 0.. {
        // serialized directly into the shm sample; never blocks
        sender.try_send(&Msg { seq, payload: vec![0; 1232] })?;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Ok(())
}
```

Consumer process (`4` = the dedicated core the busy-polling thread is pinned to):

```rust
use tokio_util::sync::CancellationToken;
use win_ipc::{IpcConfig, IpcReceiver};

fn main() -> eyre::Result<()> {
    let cancel = CancellationToken::new();

    // owned messages, handler runs on the polling thread
    let hdl = IpcReceiver::spawn_with_handler(
        "my/channel",
        &IpcConfig::default(),
        4,
        cancel.clone(),
        |m: Msg| println!("seq={} len={}", m.seq, m.payload.len()),
    )?;

    hdl.join().unwrap();
    Ok(())
}
```

Zero-copy variant — the callback gets the raw serialized bytes in place in shared memory (no deserialize, no copy; borrow only inside the callback, the shm slot is released when it returns):

```rust
let hdl = IpcReceiver::spawn_with_view_handler(
    "my/channel",
    &IpcConfig::default(),
    4,
    cancel.clone(),
    |bytes: &[u8]| {
        let m: Msg = wincode::deserialize(bytes).unwrap(); // or forward `bytes` as-is
        println!("seq={}", m.seq);
    },
)?;
```

Or bridge into tokio (drop-on-full, like a bounded mpsc):

```rust
let (mut rx, _hdl) =
    IpcReceiver::spawn::<Msg>("my/channel", &IpcConfig::default(), 4, cancel.clone())?;
while let Some(m) = rx.recv().await {
    // ...
}
```

Channel names must be unique per message type `T` (payloads are raw wincode bytes; a type mismatch surfaces as deserialize failures, not a crash).

## Demo

```bash
cargo run --example demo -- sub   # shell 1
cargo run --example demo -- pub   # shell 2
```

Kill and restart either side; the other keeps running and messages resume.
