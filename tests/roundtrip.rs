use win_ipc::{IpcConfig, IpcReceiver, IpcSender};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead, Debug, PartialEq)]
struct TestMessage {
    seq: u64,
    payload: Vec<u8>,
}

fn cfg() -> IpcConfig {
    IpcConfig::default()
}

fn wait_for(counter: &AtomicU64, target: u64) -> bool {
    for _ in 0..300 {
        if counter.load(Ordering::Relaxed) >= target {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

#[test]
fn delivers_messages() {
    let channel = "test-deliver";
    let received = Arc::new(AtomicU64::new(0));
    let cancel = CancellationToken::new();
    let r = received.clone();
    let _hdl = IpcReceiver::spawn_with_handler(channel, &cfg(), 0, cancel.clone(), move |m: TestMessage| {
        assert_eq!(m.payload.len(), 1232);
        r.fetch_add(1, Ordering::Relaxed);
        let _ = m.seq;
    })
    .unwrap();

    let sender = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();
    // subscriber connection is async; retry the first sends
    for seq in 0..50u64 {
        sender
            .try_send(&TestMessage { seq, payload: vec![1; 1232] })
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_for(&received, 40), "expected deliveries, got {}", received.load(Ordering::Relaxed));
    cancel.cancel();
}

#[test]
fn receiver_restart_does_not_break_sender() {
    let channel = "test-recv-restart";
    let cancel1 = CancellationToken::new();
    let received1 = Arc::new(AtomicU64::new(0));
    let r1 = received1.clone();
    let hdl1 = IpcReceiver::spawn_with_handler(channel, &cfg(), 0, cancel1.clone(), move |_: TestMessage| {
        r1.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();

    let sender = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();
    for seq in 0..20 {
        sender.try_send(&TestMessage { seq, payload: vec![2; 64] }).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_for(&received1, 10));

    // kill the receiver; sender must keep sending without error
    cancel1.cancel();
    hdl1.join().unwrap();
    for seq in 20..40 {
        sender.try_send(&TestMessage { seq, payload: vec![2; 64] }).unwrap();
    }

    // restart the receiver; new messages must flow again
    let cancel2 = CancellationToken::new();
    let received2 = Arc::new(AtomicU64::new(0));
    let r2 = received2.clone();
    let _hdl2 = IpcReceiver::spawn_with_handler(channel, &cfg(), 0, cancel2.clone(), move |_: TestMessage| {
        r2.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    for seq in 40..80 {
        sender.try_send(&TestMessage { seq, payload: vec![2; 64] }).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_for(&received2, 10), "restarted receiver got nothing");
    cancel2.cancel();
}

#[test]
fn sender_restart_does_not_break_receiver() {
    let channel = "test-send-restart";
    let received = Arc::new(AtomicU64::new(0));
    let cancel = CancellationToken::new();
    let r = received.clone();
    let _hdl = IpcReceiver::spawn_with_handler(channel, &cfg(), 0, cancel.clone(), move |_: TestMessage| {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();

    {
        let sender1 = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();
        for seq in 0..20 {
            sender1.try_send(&TestMessage { seq, payload: vec![3; 64] }).unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(wait_for(&received, 10));
    } // sender dropped = restart

    let sender2 = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();
    let before = received.load(Ordering::Relaxed);
    for seq in 20..60 {
        sender2.try_send(&TestMessage { seq, payload: vec![3; 64] }).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_for(&received, before + 10), "receiver got nothing from new sender");
    cancel.cancel();
}

#[test]
fn oversized_message_rejected() {
    let channel = "test-oversize";
    let sender = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();
    let big = TestMessage { seq: 1, payload: vec![0; 4096] };
    assert!(sender.try_send(&big).is_err());
}

#[test]
fn tokio_bridge_delivers() {
    let channel = "test-bridge";
    let cancel = CancellationToken::new();
    let (mut rx, _hdl) =
        IpcReceiver::spawn::<TestMessage>(channel, &cfg(), 0, cancel.clone()).unwrap();
    let sender = IpcSender::<TestMessage>::new(channel, &cfg()).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let got = rt.block_on(async move {
        let feeder = std::thread::spawn(move || {
            for seq in 0..100 {
                let _ = sender.try_send(&TestMessage { seq, payload: vec![4; 32] });
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        feeder.join().unwrap();
        msg
    });
    assert!(matches!(got, Ok(Some(_))), "bridge delivered nothing");
    cancel.cancel();
}
