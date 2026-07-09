//! Test-only logger, so debug! and trace! arguments are evaluated and
//! formatted (and counted by coverage) while the tests run.

use alloc::string::ToString;

use log::{LevelFilter, Log, Metadata, Record};

struct SinkLogger;

impl Log for SinkLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        // format the record (running every Display impl) and discard it
        let _ = record.args().to_string();
    }

    fn flush(&self) {}
}

/// Installs a trace-level sink logger; safe to call from every test.
pub(crate) fn init() {
    static LOGGER: SinkLogger = SinkLogger;
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(LevelFilter::Trace);
}
