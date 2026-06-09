#![cfg(feature = "loom-tests")]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Condvar, Mutex};
use loom::thread;

#[test]
fn commit_waiter_notification_is_not_lost() {
    eprintln!("loom deterministic scheduler: commit_waiter_notification_is_not_lost");

    loom::model(|| {
        let committed = Arc::new((Mutex::new(0usize), Condvar::new()));

        let waiter_committed = Arc::clone(&committed);
        let waiter = thread::spawn(move || {
            let (lock, cvar) = &*waiter_committed;
            let mut sequence = lock.lock().unwrap();
            while *sequence < 2 {
                sequence = cvar.wait(sequence).unwrap();
            }
            assert_eq!(*sequence, 2);
        });

        let advancer_committed = Arc::clone(&committed);
        let advancer = thread::spawn(move || {
            let (lock, cvar) = &*advancer_committed;
            let mut sequence = lock.lock().unwrap();
            *sequence = 2;
            cvar.notify_all();
        });

        advancer.join().unwrap();
        waiter.join().unwrap();
    });
}

#[test]
fn acknowledgement_is_after_durable_and_quorum_frontiers() {
    eprintln!(
        "loom deterministic scheduler: acknowledgement_is_after_durable_and_quorum_frontiers"
    );

    loom::model(|| {
        let durable = Arc::new(AtomicBool::new(false));
        let quorum = Arc::new(AtomicBool::new(false));
        let acknowledged = Arc::new(AtomicBool::new(false));

        let writer_durable = Arc::clone(&durable);
        let writer_quorum = Arc::clone(&quorum);
        let writer_acknowledged = Arc::clone(&acknowledged);
        let writer = thread::spawn(move || {
            writer_durable.store(true, Ordering::Release);
            writer_quorum.store(true, Ordering::Release);
            writer_acknowledged.store(true, Ordering::Release);
        });

        let observer_durable = Arc::clone(&durable);
        let observer_quorum = Arc::clone(&quorum);
        let observer_acknowledged = Arc::clone(&acknowledged);
        let observer = thread::spawn(move || {
            while !observer_acknowledged.load(Ordering::Acquire) {
                thread::yield_now();
            }
            assert!(observer_durable.load(Ordering::Acquire));
            assert!(observer_quorum.load(Ordering::Acquire));
        });

        writer.join().unwrap();
        observer.join().unwrap();
    });
}

#[test]
fn read_index_never_advances_beyond_committed_sequence() {
    eprintln!("loom deterministic scheduler: read_index_never_advances_beyond_committed_sequence");

    loom::model(|| {
        let local_applied = Arc::new(AtomicUsize::new(0));
        let commit_sequence = Arc::new(AtomicUsize::new(0));
        let read_index = Arc::new(AtomicUsize::new(0));

        let applier_local = Arc::clone(&local_applied);
        let applier = thread::spawn(move || {
            applier_local.store(2, Ordering::Release);
        });

        let committer_local = Arc::clone(&local_applied);
        let committer_commit = Arc::clone(&commit_sequence);
        let committer_read_index = Arc::clone(&read_index);
        let committer = thread::spawn(move || {
            while committer_local.load(Ordering::Acquire) < 2 {
                thread::yield_now();
            }
            committer_commit.store(2, Ordering::Release);
            let committed = committer_commit.load(Ordering::Acquire);
            committer_read_index.store(committed, Ordering::Release);
        });

        let reader_local = Arc::clone(&local_applied);
        let reader_commit = Arc::clone(&commit_sequence);
        let reader_read_index = Arc::clone(&read_index);
        let reader = thread::spawn(move || {
            while reader_read_index.load(Ordering::Acquire) == 0 {
                thread::yield_now();
            }
            let indexed = reader_read_index.load(Ordering::Acquire);
            let committed = reader_commit.load(Ordering::Acquire);
            let local = reader_local.load(Ordering::Acquire);
            assert!(indexed <= committed);
            assert!(indexed <= local);
        });

        applier.join().unwrap();
        committer.join().unwrap();
        reader.join().unwrap();
    });
}

#[test]
fn shared_batch_responses_do_not_precede_frontier_acknowledgement() {
    eprintln!(
        "loom deterministic scheduler: shared_batch_responses_do_not_precede_frontier_acknowledgement"
    );

    loom::model(|| {
        #[derive(Debug, Default)]
        struct BatchState {
            durable_sequence: usize,
            quorum_sequence: usize,
            response_one: usize,
            response_two: usize,
        }

        let batch = Arc::new((Mutex::new(BatchState::default()), Condvar::new()));

        let worker_batch = Arc::clone(&batch);
        let worker = thread::spawn(move || {
            let (lock, cvar) = &*worker_batch;
            let mut batch = lock.lock().unwrap();
            batch.durable_sequence = 2;
            batch.quorum_sequence = 2;
            batch.response_one = 1;
            batch.response_two = 2;
            cvar.notify_all();
        });

        let client_one_batch = Arc::clone(&batch);
        let client_one = thread::spawn(move || {
            let (lock, cvar) = &*client_one_batch;
            let mut batch = lock.lock().unwrap();
            while batch.response_one == 0 {
                batch = cvar.wait(batch).unwrap();
            }
            assert!(batch.durable_sequence >= batch.response_one);
            assert!(batch.quorum_sequence >= batch.response_one);
        });

        let client_two_batch = Arc::clone(&batch);
        let client_two = thread::spawn(move || {
            let (lock, cvar) = &*client_two_batch;
            let mut batch = lock.lock().unwrap();
            while batch.response_two == 0 {
                batch = cvar.wait(batch).unwrap();
            }
            assert!(batch.durable_sequence >= batch.response_two);
            assert!(batch.quorum_sequence >= batch.response_two);
        });

        worker.join().unwrap();
        client_one.join().unwrap();
        client_two.join().unwrap();
    });
}
