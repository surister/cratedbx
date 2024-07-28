use std::time::{Duration, Instant};
pub(crate) struct Metadata {
    _start: Option<Instant>,
    _last: Option<Duration>,
    total: u32,
}

impl Metadata {
    pub(crate) fn start(&mut self) {
        self._start = Some(Instant::now());
    }
    pub(crate) fn new() -> Self {
        Metadata {
            _start: None,
            _last: Some(Duration::new(0, 0)),
            total: 0,
        }
    }

    // Todo: Add new param: "track_time = false" to disable printing the time it took since the last print_step
    pub fn print_step(&mut self, message: &str) {
        println!(
            "{message}, took: {:?}", self._start.unwrap().elapsed() - self._last.unwrap()
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
