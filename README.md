# win-ipc

shared-memory IPC channels for latency-sensitive services: [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) zero-copy transport + [wincode](https://crates.io/crates/wincode) serialization.

## Properties

- **Low latency**: messages are wincode-serialized directly into the loaned shm sample (single copy, no allocation); the receiver busy-polls its own thread, pinned to a required dedicated core.
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
let hdl = IpcReceiver::spawn_with_handler("my/channel", &IpcConfig::default(), CORE, cancel, |m: Msg| {
    // ...
})?;

// or bridge into tokio
let (mut rx, hdl) = IpcReceiver::spawn::<Msg>("my/channel", &IpcConfig::default(), CORE, cancel)?;
```

Channel names must be unique per message type `T` (payloads are raw wincode bytes; a type mismatch surfaces as deserialize failures, not a crash).

## Demo

```bash
cargo run --example demo -- sub   # shell 1
cargo run --example demo -- pub   # shell 2
```

Kill and restart either side; the other keeps running and messages resume.
