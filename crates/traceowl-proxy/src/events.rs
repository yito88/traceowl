pub use traceowl_schema::event_v1::{
    DbInfoV1 as DbInfo, ErrorKindV1 as ErrorKind, EventV1 as Event, HitV1 as HitInfo,
    QueryInfoV1 as QueryInfo, RequestEventV1 as RequestEvent, ResponseEventV1 as ResponseEvent,
    ResultInfoV1 as ResultInfo, StatusInfoV1 as StatusInfo, TimingInfoV1 as TimingInfo,
};

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
