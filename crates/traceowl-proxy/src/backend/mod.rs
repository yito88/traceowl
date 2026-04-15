pub mod hashing;
pub mod pinecone;
pub mod qdrant;

use http::Method;

use crate::events::{DbInfo, HitInfo, QueryInfo};

/// Metadata extracted when a backend matches a request.
pub struct RequestMatch {
    /// The VectorDB kind (e.g. "qdrant", "pinecone")
    pub db_kind: String,
    /// Collection/index name
    pub collection: String,
    /// The original request path (used to re-find the backend after forwarding)
    pub path: String,
}

impl RequestMatch {
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Parsed request metadata for event emission.
pub struct RequestMeta {
    pub unsupported_shape: bool,
    pub query: QueryInfo,
    /// Overrides `RequestMatch::collection` when building `DbInfo`.
    /// Used by adapters (e.g. Pinecone) that cannot determine the collection
    /// from the URL path alone and extract it from the request body instead.
    pub collection_override: Option<String>,
}

/// Parsed response metadata for event emission.
pub struct ResponseMeta {
    pub hits: Vec<HitInfo>,
}

/// Trait for VectorDB-specific request/response handling.
///
/// Each backend implementation knows how to:
/// - Match incoming requests to its supported endpoints
/// - Parse request bodies into event metadata
/// - Parse response bodies into event metadata
///
/// The proxy core calls these methods and handles sampling, queuing,
/// and event emission generically.
pub trait BackendHandler: Send + Sync {
    /// Try to match this request. Returns `Some(RequestMatch)` if this
    /// backend handles the given method+path, `None` otherwise.
    fn match_request(&self, method: &Method, path: &str) -> Option<RequestMatch>;

    /// Parse the request body into event metadata.
    /// Called only when `match_request` returned `Some`.
    fn parse_request(&self, matched: &RequestMatch, body: &[u8]) -> RequestMeta;

    /// Parse the response body into event metadata.
    /// Called only when `match_request` returned `Some`.
    fn parse_response(&self, matched: &RequestMatch, status: u16, body: &[u8]) -> ResponseMeta;
}

/// Build a `DbInfo` from a `RequestMatch`.
impl From<&RequestMatch> for DbInfo {
    fn from(m: &RequestMatch) -> Self {
        DbInfo {
            kind: m.db_kind.clone(),
            collection: m.collection.clone(),
        }
    }
}
