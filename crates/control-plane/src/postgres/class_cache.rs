//! A stored workload class version never changes, so a process keeps the ones
//! it has read and reads each from the database once.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::workload::{WorkloadClassVersion, WorkloadClassVersionRef};

/// Class versions one process keeps. An operator creates classes; a full
/// cache drops an entry, which the next use reads again.
const CAPACITY: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct ClassCache {
    classes: Mutex<HashMap<(String, u64), Arc<WorkloadClassVersion>>>,
}

fn key(reference: &WorkloadClassVersionRef) -> (String, u64) {
    (
        reference.class_id.as_str().to_owned(),
        reference.version.get(),
    )
}

impl ClassCache {
    pub(crate) fn get(&self, reference: &WorkloadClassVersionRef) -> Option<WorkloadClassVersion> {
        let classes = self.classes.lock().expect("class cache");
        classes
            .get(&key(reference))
            .map(|class| WorkloadClassVersion::clone(class))
    }

    pub(crate) fn insert(&self, class: &WorkloadClassVersion) {
        let mut classes = self.classes.lock().expect("class cache");
        insert_bounded(&mut classes, key(&class.reference), Arc::new(class.clone()));
    }
}

/// Inserts `value`, first dropping an entry when `map` is full.
fn insert_bounded<K: Clone + Eq + std::hash::Hash, V>(map: &mut HashMap<K, V>, key: K, value: V) {
    if map.len() >= CAPACITY && !map.contains_key(&key) {
        if let Some(evicted) = map.keys().next().cloned() {
            map.remove(&evicted);
        }
    }
    map.insert(key, value);
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_cache_stays_within_its_capacity() {
        let mut map = std::collections::HashMap::new();
        for key in 0..super::CAPACITY + 10 {
            super::insert_bounded(&mut map, key, ());
        }
        assert_eq!(map.len(), super::CAPACITY);
        assert!(map.contains_key(&(super::CAPACITY + 9)));
    }
}
