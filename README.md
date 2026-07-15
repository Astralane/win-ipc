# win-ipc

Restart-safe shared-memory IPC channels for latency-sensitive services: [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) zero-copy transport + [wincode](https://crates.io/crates/wincode) serialization.

Built to replace unix-domain-socket hops between co-located processes (proxy → router → broadcaster) with a single-copy shared-memory path.

## Properties

- **Restart-safe**: either side can crash/restart freely. Services are `open_or_create`, publishing with no subscriber is a no-op, stale resources of dead nodes are swept on startup.
- **Low latency**: messages are wincode-serialized directly into the loaned shm sample (single copy, no allocation); receiver supports `BusySpin`, `SpinThenWait` (default), or `Event` wait modes with optional core pinning.
- **Never blocks the sender**: safe-overflow queues drop oldest when a subscriber stalls.

## Usage

```rust
use win_ipc::{IpcConfig, IpcReceiver, IpcSender};
use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead)]
struct Msg { seq: u64, payload: Vec<u8> }

// process A
let sender = IpcSender::<Msg>::new("my/channel", &IpcConfig::default())?;
sender.try_send(&Msg { seq: 1, payload: vec![0; 1232] })?;

// process B (handler runs on the polling thread — lowest latency)
let hdl = IpcReceiver::spawn_with_handler("my/channel", &IpcConfig::default(), cancel, |m: Msg| {
    // ...
})?;

// or bridge into tokio
let (mut rx, hdl) = IpcReceiver::spawn::<Msg>("my/channel", &IpcConfig::default(), cancel)?;
```

Channel names must be unique per message type `T` (payloads are raw wincode bytes; a type mismatch surfaces as deserialize failures, not a crash).

## Demo

```bash
cargo run --example demo -- sub   # shell 1
cargo run --example demo -- pub   # shell 2
```

Kill and restart either side; the other keeps running and messages resume.
