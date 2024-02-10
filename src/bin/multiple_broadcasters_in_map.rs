use futures_signals::{
    signal::{Signal, SignalExt},
    signal_map::MutableBTreeMap,
    signal_vec::SignalVecExt,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::{sync::RwLock, time::sleep};

use futures_signals::signal::Broadcaster;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // using Arc<String> to get cheap clones.  In my app the items can be large (and read only) and I don't want to clone them
    let source: Arc<MutableBTreeMap<String, Arc<String>>> = Arc::new(MutableBTreeMap::new());

    // using Arc<Vec...> to get cheap clones.  In my app the items can be large (and read only) and I don't want to clone them
    let select_cache: Arc<
        RwLock<
            HashMap<
                usize,
                Broadcaster<Pin<Box<dyn Signal<Item = Arc<Vec<Arc<String>>>> + Send + Sync>>>,
            >,
        >,
    > = Default::default();

    let num_finished = Arc::new(AtomicUsize::new(0));

    const NUM_KEYS: usize = 4; // the number of broadcasters
    const NUM_VALUES: usize = 10; // number of values per source key/broadcaster

    // create broadcasters
    for i in 0..NUM_KEYS {
        let b = broadcaster(i, source.clone());
        select_cache.write().await.insert(i, b);
    }

    // track broadcaster for completion

    for i in 0..NUM_KEYS {
        let b = select_cache.read().await.get(&i).unwrap().clone();
        track_finished(b, num_finished.clone(), NUM_VALUES);
    }

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
    for value in 0..num_values {
        for key in 0..num_keys {
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
