//! Cross-process demo: `cargo run -p shm-ipc --example demo -- pub` in one
//! shell, `-- sub` in another. Kill/restart either side freely.
use win_ipc::{IpcConfig, IpcReceiver, IpcSender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wincode::{SchemaRead, SchemaWrite};

const CHANNEL: &str = "demo";

#[derive(SchemaWrite, SchemaRead, Debug)]
struct DemoMessage {
    seq: u64,
    created_at_nanos: u64,
    payload: Vec<u8>,
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn main() -> eyre::Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let cfg = IpcConfig::default();
    match mode.as_str() {
        "pub" => {
            let sender = IpcSender::<DemoMessage>::new(CHANNEL, &cfg)?;
            let mut seq = 0u64;
            loop {
                seq += 1;
                let msg = DemoMessage {
                    seq,
                    created_at_nanos: now_nanos(),
                    payload: vec![0xAB; 1232],
                };
                match sender.try_send(&msg) {
                    Ok(_) => println!("sent seq={seq}"),
                    Err(e) => println!("send failed seq={seq}: {e:?}"),
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
        "sub" => {
            let core = std::env::args()
                .nth(2)
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            let last = AtomicU64::new(0);
            let hdl = IpcReceiver::spawn_with_handler(
                CHANNEL,
                &cfg,
                core,
                CancellationToken::new(),
                move |msg: DemoMessage| {
                    let transit_us = now_nanos().saturating_sub(msg.created_at_nanos) / 1_000;
                    let prev = last.swap(msg.seq, Ordering::Relaxed);
                    let gap = if prev != 0 && msg.seq != prev + 1 { " GAP" } else { "" };
                    println!(
                        "recv seq={} transit={}us len={}{gap}",
                        msg.seq,
                        transit_us,
                        msg.payload.len()
                    );
                },
            )?;
            hdl.join().unwrap();
            Ok(())
        }
        _ => {
            eprintln!("usage: demo <pub|sub>");
            std::process::exit(1);
        }
    }
}
