use std::time::{Duration, Instant};

use sleepypods_api::{HostRules, RouteKind};

use super::{HostKey, RouteTable};
use crate::route::testing::{http_rule, running};

fn key(host: &str) -> HostKey {
    HostKey::new(RouteKind::Http, host)
}

fn rules(host: &str, instance: &str) -> HostRules {
    HostRules::ranked(vec![http_rule(host, None, running(instance))])
}

fn install(
    table: &mut RouteTable,
    host: &str,
    subscription: u64,
    now: Instant,
) -> super::Displaced {
    table.install(
        key(host),
        rules(host, "instance"),
        now + Duration::from_secs(60),
        Some(subscription),
    )
}

fn cached(table: &mut RouteTable, host: &str) -> bool {
    table.publish();
    table.get(&key(host)).is_some()
}

#[test]
fn a_published_snapshot_holds_what_was_installed_and_stays_as_it_was() {
    let now = Instant::now();
    let mut table = RouteTable::new(4);
    assert!(table.publish().is_none(), "nothing changed yet");

    install(&mut table, "a.example.com", 1, now);
    let first = table.publish().expect("the table changed");
    assert!(first.get(RouteKind::Http, "a.example.com").is_some());
    assert!(first.get(RouteKind::Sni, "a.example.com").is_none());
    assert!(table.publish().is_none());

    install(&mut table, "b.example.com", 2, now);
    let second = table.publish().expect("the table changed");
    assert_eq!(second.len(), 2);
    assert_eq!(first.len(), 1, "a published snapshot never changes");
}

#[test]
fn rule_sets_leave_first_in_first_out_and_release_their_subscriptions() {
    let now = Instant::now();
    let mut table = RouteTable::new(2);
    install(&mut table, "a.example.com", 1, now);
    install(&mut table, "b.example.com", 2, now);

    let displaced = install(&mut table, "c.example.com", 3, now);

    assert_eq!(displaced.unsubscribe, vec![1]);
    assert_eq!(displaced.entries.len(), 1);
    assert!(!cached(&mut table, "a.example.com"));
    assert!(cached(&mut table, "b.example.com"));
    assert!(cached(&mut table, "c.example.com"));
    assert_eq!(table.positive_len(), 2);
}

#[test]
fn a_replaced_rule_set_keeps_its_place_and_its_subscription() {
    let now = Instant::now();
    let mut table = RouteTable::new(2);
    install(&mut table, "a.example.com", 1, now);
    install(&mut table, "b.example.com", 2, now);
    let before = table.get(&key("a.example.com")).unwrap().version;

    // The same subscription delivers a new rule set for the oldest host.
    let displaced = install(&mut table, "a.example.com", 1, now);
    assert!(displaced.unsubscribe.is_empty());
    assert_eq!(
        displaced.entries.len(),
        1,
        "the replaced answer is reported"
    );
    assert!(table.get(&key("a.example.com")).unwrap().version > before);

    // It is still the oldest, so it is the one that leaves.
    let displaced = install(&mut table, "c.example.com", 3, now);
    assert_eq!(displaced.unsubscribe, vec![1]);
    assert!(cached(&mut table, "b.example.com"));
}

#[test]
fn a_new_subscription_for_a_cached_host_releases_the_old_one() {
    let now = Instant::now();
    let mut table = RouteTable::new(2);
    install(&mut table, "a.example.com", 1, now);

    let displaced = install(&mut table, "a.example.com", 9, now);

    assert_eq!(displaced.unsubscribe, vec![1]);
    assert_eq!(table.host_of(9), Some(&key("a.example.com")));
    assert_eq!(table.host_of(1), None);
}

#[test]
fn empty_answers_have_their_own_budget_and_hold_no_subscription() {
    let now = Instant::now();
    let mut table = RouteTable::new(2);
    install(&mut table, "a.example.com", 1, now);
    install(&mut table, "b.example.com", 2, now);

    for (index, host) in ["x", "y", "z"].into_iter().enumerate() {
        let displaced = table.install(
            key(&format!("{host}.example.com")),
            HostRules::default(),
            now + Duration::from_secs(1),
            Some(10 + index as u64),
        );
        assert!(displaced.unsubscribe.is_empty());
    }

    assert_eq!(
        table.positive_len(),
        2,
        "hosts with no route push no route out"
    );
    assert_eq!(table.negative_len(), 2);
    assert!(!cached(&mut table, "x.example.com"));
    assert_eq!(table.host_of(12), None);
}

#[test]
fn a_rule_set_that_empties_stops_counting_as_a_route_and_keeps_no_subscription() {
    let now = Instant::now();
    let mut table = RouteTable::new(2);
    install(&mut table, "a.example.com", 1, now);

    let displaced = table.install(
        key("a.example.com"),
        HostRules::default(),
        now + Duration::from_secs(1),
        Some(1),
    );

    assert!(
        displaced.unsubscribe.is_empty(),
        "the control plane already dropped it"
    );
    assert_eq!((table.positive_len(), table.negative_len()), (0, 1));
    assert_eq!(table.host_of(1), None);
    assert!(table.get(&key("a.example.com")).unwrap().rules.is_empty());
}

#[test]
fn answers_expire_in_deadline_order_up_to_the_limit() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    for (index, host) in ["a", "b", "c"].into_iter().enumerate() {
        table.install(
            key(&format!("{host}.example.com")),
            rules(&format!("{host}.example.com"), "instance"),
            now + Duration::from_secs(index as u64 + 1),
            Some(index as u64 + 1),
        );
    }
    assert_eq!(table.next_expiry(), Some(now + Duration::from_secs(1)));

    assert!(table.expire(now, usize::MAX).entries.is_empty());
    let displaced = table.expire(now + Duration::from_secs(2), 1);
    assert_eq!(displaced.unsubscribe, vec![1]);
    let displaced = table.expire(now + Duration::from_secs(2), usize::MAX);
    assert_eq!(displaced.unsubscribe, vec![2]);
    assert!(cached(&mut table, "c.example.com"));
}

#[test]
fn an_ended_session_keeps_used_rule_sets_unregistered_and_drops_the_rest() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    install(&mut table, "used.example.com", 1, now);
    install(&mut table, "idle.example.com", 2, now);
    table.get(&key("used.example.com")).unwrap().mark_used();

    let (kept, displaced) = table.end_session();

    assert_eq!(kept, vec![key("used.example.com")]);
    assert!(displaced.unsubscribe.is_empty(), "the stream is gone");
    assert_eq!(displaced.entries.len(), 1);
    assert!(table.awaits_registration(&key("used.example.com"), now));
    assert!(!cached(&mut table, "idle.example.com"));
    assert_eq!(table.host_of(1), None);
    let carried = table.get(&key("used.example.com")).unwrap();
    assert_eq!(carried.subscription(), None);
    assert!(
        !carried.was_used(),
        "use is counted afresh on the next stream"
    );

    // Registering it again makes it live under the new subscription.
    install(&mut table, "used.example.com", 7, now);
    assert!(!table.awaits_registration(&key("used.example.com"), now));
    assert_eq!(table.host_of(7), Some(&key("used.example.com")));
}

#[test]
fn a_carried_rule_set_keeps_its_deadline() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    install(&mut table, "a.example.com", 1, now);
    table.get(&key("a.example.com")).unwrap().mark_used();
    table.end_session();

    assert!(table.awaits_registration(&key("a.example.com"), now + Duration::from_secs(59)));
    assert!(!table.awaits_registration(&key("a.example.com"), now + Duration::from_secs(60)));
    let displaced = table.expire(now + Duration::from_secs(60), usize::MAX);
    assert_eq!(displaced.entries.len(), 1);
    assert!(displaced.unsubscribe.is_empty());
}

#[test]
fn removing_an_ended_subscription_drops_its_host_and_asks_for_no_release() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    install(&mut table, "a.example.com", 1, now);
    install(&mut table, "b.example.com", 2, now);

    let displaced = table.remove_subscription(1);

    assert!(displaced.unsubscribe.is_empty());
    assert_eq!(displaced.entries.len(), 1);
    assert!(!cached(&mut table, "a.example.com"));
    assert!(cached(&mut table, "b.example.com"));
    assert!(table.remove_subscription(99).entries.is_empty());
}

#[test]
fn a_lowered_capacity_holds_for_later_installs() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    for index in 0..4 {
        install(&mut table, &format!("h{index}.example.com"), index + 1, now);
    }

    let displaced = table.evict_oldest(2);
    assert_eq!(displaced.unsubscribe, vec![1, 2]);
    table.limit_capacity(table.positive_len());
    assert_eq!(table.capacity(), 2);

    let displaced = install(&mut table, "new.example.com", 9, now);
    assert_eq!(displaced.unsubscribe, vec![3]);
    assert_eq!(table.positive_len(), 2);
}

#[test]
fn clearing_reports_every_answer_and_empties_the_snapshot() {
    let now = Instant::now();
    let mut table = RouteTable::new(8);
    install(&mut table, "a.example.com", 1, now);
    table.install(
        key("none.example.com"),
        HostRules::default(),
        now + Duration::from_secs(1),
        None,
    );

    let displaced = table.clear();

    assert_eq!(displaced.entries.len(), 2);
    assert!(table.publish().expect("the table changed").is_empty());
    assert_eq!((table.positive_len(), table.negative_len()), (0, 0));
    assert_eq!(table.next_expiry(), None);
}
