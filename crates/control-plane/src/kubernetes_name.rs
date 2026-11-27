use crate::ids::{InstanceId, DNS_LABEL_MAX_LEN};
use sha2::{Digest, Sha256};

/// A StatefulSet stamps `<name>-<revision hash>` on its Pods as a label value,
/// which leaves the name 52 of a label's 63 characters.
pub(crate) const STATEFUL_SET_NAME_MAX_LEN: usize = 52;
const INSTANCE_ID_HASH_HEX_LEN: usize = 16;
const INSTANCE_ID_SEPARATOR_LEN: usize = 1;

pub fn render_instance_scoped_name(base: &str, instance_id: &InstanceId) -> String {
    let suffix = instance_suffix(instance_id);
    let base_budget = DNS_LABEL_MAX_LEN - INSTANCE_ID_SEPARATOR_LEN - suffix.len();
    let base = truncate_to_byte_len(base, base_budget);

    format!("{base}-{suffix}")
}

/// The hash every instance-scoped object name ends in.
pub(crate) fn instance_suffix(instance_id: &InstanceId) -> String {
    let digest = Sha256::digest(instance_id.as_str().as_bytes());
    digest
        .iter()
        .take(INSTANCE_ID_HASH_HEX_LEN / 2)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn truncate_to_byte_len(value: &str, max_len: usize) -> &str {
    if value.len() <= max_len {
        return value;
    }

    let mut end = 0;
    for (index, ch) in value.char_indices() {
        let next = index + ch.len_utf8();
        if next > max_len {
            break;
        }
        end = next;
    }

    &value[..end]
}

#[cfg(test)]
mod tests {
    use crate::ids::{is_dns_label, InstanceId, DNS_LABEL_MAX_LEN};

    use super::render_instance_scoped_name;

    #[test]
    fn instance_scoped_name_uses_stable_instance_id_hash_suffix() {
        assert_eq!(
            render_instance_scoped_name("api", &instance_id("inst")),
            "api-9db333d855b89d61"
        );
    }

    #[test]
    fn instance_scoped_name_hashes_full_instance_id() {
        assert_eq!(
            render_instance_scoped_name("api", &instance_id("instance-a")),
            "api-69856ec0faae6daf"
        );
    }

    #[test]
    fn instance_scoped_name_separates_prefix_sharing_instance_ids() {
        let first = render_instance_scoped_name("api", &instance_id("tenant-0001"));
        let second = render_instance_scoped_name("api", &instance_id("tenant-0002"));

        assert_eq!(first, "api-4c1eaa5b6e9da93b");
        assert_eq!(second, "api-1f884bb8f1ed8c1a");
        assert_ne!(first, second);
    }

    #[test]
    fn instance_scoped_name_truncates_base_before_suffix() {
        let rendered = render_instance_scoped_name(&"a".repeat(80), &instance_id("instance-a"));

        assert_eq!(rendered.len(), DNS_LABEL_MAX_LEN);
        assert!(rendered.ends_with("-69856ec0faae6daf"));
        assert!(is_dns_label(&rendered));
    }

    #[test]
    fn colliding_32_bit_prefixes_render_distinct_names() {
        // These IDs share the first 32 bits of their SHA-256 digests.
        assert_ne!(
            render_instance_scoped_name("web", &instance_id("tenant-39588")),
            render_instance_scoped_name("web", &instance_id("tenant-67738")),
        );
    }

    #[test]
    fn one_hundred_thousand_sequential_instance_ids_render_distinct_names() {
        assert_distinct_names(100_000);
    }

    #[test]
    #[ignore = "hashes ten million IDs; run with --release -- --ignored"]
    fn ten_million_sequential_instance_ids_render_distinct_names() {
        assert_distinct_names(10_000_000);
    }

    fn assert_distinct_names(count: usize) {
        let mut names = (0..count)
            .map(|index| {
                render_instance_scoped_name("web", &instance_id(&format!("tenant-{index}")))
            })
            .collect::<Vec<_>>();
        names.sort_unstable();
        let unique = names.len();
        names.dedup();
        assert_eq!(names.len(), unique, "instance-scoped names collided");
    }

    fn instance_id(value: &str) -> InstanceId {
        InstanceId::new(value).expect("valid instance ID")
    }
}
