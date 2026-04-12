use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub enum ErrorKind {
    #[serde(rename = "upstream_timeout")]
    UpstreamTimeout,
    #[serde(rename = "upstream_connect_error")]
    UpstreamConnectError,
    #[serde(rename = "upstream_5xx")]
    Upstream5xx,
    #[serde(rename = "decode_error")]
    DecodeError,
    #[serde(rename = "internal_proxy_error")]
    InternalProxyError,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::UpstreamTimeout => write!(f, "upstream_timeout"),
            ErrorKind::UpstreamConnectError => write!(f, "upstream_connect_error"),
            ErrorKind::Upstream5xx => write!(f, "upstream_5xx"),
            ErrorKind::DecodeError => write!(f, "decode_error"),
            ErrorKind::InternalProxyError => write!(f, "internal_proxy_error"),
        }
    }
}
