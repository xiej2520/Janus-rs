use std::process;

pub fn coordinator_socket_name() -> String {
    format!("/var/tmp/janus-mr-{}", process::id())
}
