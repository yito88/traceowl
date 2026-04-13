use traceowl_schema::event_v1::EventV1;

pub fn parse_event(line: &str) -> Result<EventV1, serde_json::Error> {
    serde_json::from_str(line)
}
