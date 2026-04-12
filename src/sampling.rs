use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Deterministic sampling based on hash of the request_id.
/// Returns true if the request should be sampled.
pub fn is_sampled(request_id: &Uuid, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    let hash = Sha256::digest(request_id.as_bytes());
    let val = u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]);
    (val as f64 / u32::MAX as f64) < rate
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_zero_never_samples() {
        for _ in 0..100 {
            let id = Uuid::now_v7();
            assert!(!is_sampled(&id, 0.0));
        }
    }

    #[test]
    fn test_rate_one_always_samples() {
        for _ in 0..100 {
            let id = Uuid::now_v7();
            assert!(is_sampled(&id, 1.0));
        }
    }

    #[test]
    fn test_deterministic() {
        let id = Uuid::now_v7();
        let result1 = is_sampled(&id, 0.5);
        let result2 = is_sampled(&id, 0.5);
        assert_eq!(result1, result2);
    }
}
