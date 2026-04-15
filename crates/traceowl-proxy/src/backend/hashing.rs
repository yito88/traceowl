use sha2::{Digest, Sha256};

/// The extracted query representation from a VectorDB request.
/// Internal to the adapter layer — not part of the public schema.
#[allow(dead_code)]
pub enum QueryRepresentation {
    Text(String),
    Vector(Vec<f32>),
}

/// Normalize query text by collapsing whitespace.
pub fn normalize_query_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<&str>>().join(" ")
}

/// Normalize a query vector to a stable integer representation.
///
/// Each float is rounded to 3 decimal places and converted to i32
/// (e.g. 0.1234 → 123, -0.5678 → -568). This makes the hash stable
/// across minor float formatting differences.
pub fn normalize_query_vector(vec: &[f32]) -> Vec<i32> {
    vec.iter().map(|v| (v * 1000.0).round() as i32).collect()
}

/// Compute a stable query hash from scope, top_k, and query representation.
///
/// Hash inputs (in order):
///   scope_primary bytes
///   scope_secondary bytes (if present)
///   top_k as little-endian u64
///   normalized representation:
///     - Text: UTF-8 bytes of whitespace-normalized text
///     - Vector: each normalized i32 as little-endian bytes
pub fn compute_query_hash(
    scope_primary: &str,
    scope_secondary: Option<&str>,
    top_k: u64,
    repr: &QueryRepresentation,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope_primary.as_bytes());
    if let Some(secondary) = scope_secondary {
        hasher.update(secondary.as_bytes());
    }
    hasher.update(top_k.to_le_bytes());
    match repr {
        QueryRepresentation::Text(text) => {
            hasher.update(normalize_query_text(text).as_bytes());
        }
        QueryRepresentation::Vector(vec) => {
            for v in normalize_query_vector(vec) {
                hasher.update(v.to_le_bytes());
            }
        }
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_normalization_stable() {
        // Different f32 values that still round to the same 3dp integers (123, 568)
        let v1 = vec![0.1234_f32, 0.5678_f32];
        let v2 = vec![0.1233_f32, 0.5677_f32];
        assert_eq!(normalize_query_vector(&v1), normalize_query_vector(&v2));
    }

    #[test]
    fn test_vector_normalization_values() {
        let v = vec![0.1_f32, -0.5_f32, 0.999_f32];
        let norm = normalize_query_vector(&v);
        assert_eq!(norm[0], 100);
        assert_eq!(norm[1], -500);
        assert_eq!(norm[2], 999);
    }

    #[test]
    fn test_hash_deterministic_vector() {
        let repr = QueryRepresentation::Vector(vec![0.1, 0.2, 0.3]);
        let h1 = compute_query_hash("col", None, 10, &repr);
        let h2 = compute_query_hash("col", None, 10, &repr);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_stable_across_float_formatting() {
        // Different f32 values that round to the same 3dp integers (123, 568)
        let repr1 = QueryRepresentation::Vector(vec![0.1234_f32, 0.5678_f32]);
        let repr2 = QueryRepresentation::Vector(vec![0.1233_f32, 0.5677_f32]);
        let h1 = compute_query_hash("col", None, 10, &repr1);
        let h2 = compute_query_hash("col", None, 10, &repr2);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_different_for_different_vectors() {
        let repr1 = QueryRepresentation::Vector(vec![0.1, 0.2, 0.3]);
        let repr2 = QueryRepresentation::Vector(vec![0.4, 0.5, 0.6]);
        assert_ne!(
            compute_query_hash("col", None, 10, &repr1),
            compute_query_hash("col", None, 10, &repr2)
        );
    }

    #[test]
    fn test_hash_different_for_different_scope() {
        let repr = QueryRepresentation::Vector(vec![0.1, 0.2]);
        assert_ne!(
            compute_query_hash("col_a", None, 10, &repr),
            compute_query_hash("col_b", None, 10, &repr)
        );
    }

    #[test]
    fn test_hash_secondary_scope_changes_hash() {
        let repr = QueryRepresentation::Vector(vec![0.1, 0.2]);
        let h_no_ns = compute_query_hash("idx", None, 10, &repr);
        let h_ns = compute_query_hash("idx", Some("my-ns"), 10, &repr);
        assert_ne!(h_no_ns, h_ns);
    }

    #[test]
    fn test_hash_deterministic_text() {
        let repr = QueryRepresentation::Text("hello world".to_string());
        let h1 = compute_query_hash("col", None, 5, &repr);
        let h2 = compute_query_hash("col", None, 5, &repr);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_text_normalization() {
        assert_eq!(normalize_query_text("  hello   world  "), "hello world");
    }
}
