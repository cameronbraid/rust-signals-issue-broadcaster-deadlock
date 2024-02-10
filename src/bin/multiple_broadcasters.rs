use futures_signals::{
    signal::{Signal, SignalExt},
    signal_map::MutableBTreeMap,
    signal_vec::SignalVecExt,
};
use std::{
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::time::sleep;

use futures_signals::signal::Broadcaster;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // using Arc<String> to get cheap clones.  In my app the items can be large (and read only) and I don't want to clone them
    let source: Arc<MutableBTreeMap<String, Arc<String>>> = Arc::new(MutableBTreeMap::new());

    let num_finished = Arc::new(AtomicUsize::new(0));

    const NUM_KEYS: usize = 4; // must match the number of broadcasters statically defined below both creation and track_finished
    const NUM_VALUES: usize = 50; // number of values per source key/broadcaster, can increase this to increase the chance if it not finishing

    // create broadcasters

    let b_0 = broadcaster(0, source.clone());
    let b_1 = broadcaster(1, source.clone());
    let b_2 = broadcaster(2, source.clone());
    let b_3 = broadcaster(3, source.clone());

    // track broadcaster for completion

    track_finished(b_0.clone(), num_finished.clone(), NUM_VALUES);
    track_finished(b_1.clone(), num_finished.clone(), NUM_VALUES);
    track_finished(b_2.clone(), num_finished.clone(), NUM_VALUES);
    track_finished(b_3.clone(), num_finished.clone(), NUM_VALUES);

    populate_source(source.clone(), NUM_VALUES, NUM_KEYS);

    loop {
        let num = num_finished.load(std::sync::atomic::Ordering::SeqCst);
        println!("num_finished {}", num);
        if num == NUM_KEYS {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    println!("Done")
}

fn populate_source(
    source: Arc<MutableBTreeMap<String, Arc<String>>>,
    num_values: usize,
    num_keys: usize,
) {
    let mut source = source.lock_mut();
    for key in 0..num_keys {
        for value in 0..num_values {
            // let mut source = source.lock_mut();
            source.insert_cloned(format!("{key}-{value}"), Arc::new(format!("{key}-{value}")));
        }
    }
    eprintln!("source populated");
  }

fn broadcaster(
    key: usize,
    source: Arc<MutableBTreeMap<String, Arc<String>>>,
) -> Broadcaster<Pin<Box<dyn Signal<Item = Arc<Vec<Arc<String>>>> + Send + Sync>>> {
    let with_prefix = source
        .entries_cloned()
        .filter_map(move |(k, v)| {
            if k.starts_with(&format!("{key}-")) {
                Some(v)
            } else {
                None
            }
        })
        .to_signal_cloned()
        .map(|v| Arc::new(v));

    Broadcaster::new(Box::pin(with_prefix))
}

fn track_finished(
    b: Broadcaster<Pin<Box<dyn Signal<Item = Arc<Vec<Arc<String>>>> + Send + Sync>>>,
    num_finished: Arc<AtomicUsize>,
    num_values: usize,
) {
    tokio::spawn(b.signal_cloned().for_each(move |v| {
        if v.len() == num_values {
            num_finished.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        async {}
    }));
}
