#![no_main]

use interactive_dag::naive::{fuzz::*, WrappedDag};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: Input| {
    let mut graph = WrappedDag::default();
    for method in input.methods {
        graph.run(method);
    }
});
