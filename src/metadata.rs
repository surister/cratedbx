use std::time::{Duration, Instant};
use memory_stats::memory_stats;

pub(crate) struct Metadata {
    _start: Option<Instant>,
    _last: Option<Duration>,
}

impl Metadata {
    pub(crate) fn start(&mut self) {
        self._start = Some(Instant::now());
    }
    pub(crate) fn new() -> Self {
        Metadata {
            _start: None,
            _last: Some(Duration::new(0, 0)),
        }
    }

    // Todo: Add new param: "track_time = false" to disable printing the time it took since the last print_step
    pub fn print_step(&mut self, message: &str) {
        let base: usize = 10;
        println!(
            "{message}, took: {:?} - Current mem use: {:?}Mb",
            self._start.unwrap().elapsed() - self._last.unwrap(),
            (memory_stats().unwrap().physical_mem) / base.pow(6),
        );
        self._last = Some(self._start.unwrap().elapsed());
    }

    pub fn print_total_duration(&self) {
        match self._start {
            None => panic!("Cannot print total duration without calling .start() first"),
            Some(t) => println!("\nTotal elapsed: {:.2?}", t.elapsed())
        }
    }
    pub fn elapsed(&self) -> Duration {
        return self._start.unwrap().elapsed();
    }
}
