use crate::util::KeyValue;

pub mod crash;
pub mod early_exit;
pub mod indexer;
pub mod jobcount;
pub mod mtiming;
pub mod nocrash;
pub mod rtiming;
pub mod wc;

pub type MapFn = fn(&str, &str) -> Vec<KeyValue>;
pub type ReduceFn = fn(&str, Vec<&str>) -> String;

pub fn get_app(name: &str) -> (MapFn, ReduceFn) {
    match name {
        "wc" => (wc::map, wc::reduce),
        "indexer" => (indexer::map, indexer::reduce),
        "mtiming" => (mtiming::map, mtiming::reduce),
        "rtiming" => (rtiming::map, rtiming::reduce),
        "jobcount" => (jobcount::map, jobcount::reduce),
        "early_exit" => (early_exit::map, early_exit::reduce),
        "nocrash" => (nocrash::map, nocrash::reduce),
        "crash" => (crash::map, crash::reduce),
        other => {
            panic!("Invalid app {}, ending.", other);
        }
    }
}
