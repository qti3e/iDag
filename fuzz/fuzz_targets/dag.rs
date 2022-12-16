#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use poisoned_dag::test_utils::{NaiveBackedDag, Node};
use std::fmt::{Debug, Formatter, Pointer};

#[derive(Arbitrary)]
struct Input {
    methods: Vec<DagMethod>,
}

#[derive(Arbitrary)]
enum DagMethod {
    Insert { node: Node },
    Delete { node: Node },
    Connect { v: Node, u: Node },
    Disconnect { v: Node, u: Node },
}

impl Debug for DagMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DagMethod::Insert { node } => {
                write!(f, "g.insert({});", node)
            }
            DagMethod::Delete { node } => {
                write!(f, "g.delete({});", node)
            }
            DagMethod::Connect { v, u } => {
                write!(f, "g.connect({}, {});", v, u)
            }
            DagMethod::Disconnect { v, u } => {
                write!(f, "g.disconnect({}, {});", v, u)
            }
        }
    }
}

impl Debug for Input {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        for method in &self.methods {
            result.push_str(&format!("{:?}", method));
            result.push_str("\n");
        }

        f.write_str(result.as_str())
    }
}

fuzz_target!(|input: Input| {
    let mut graph = NaiveBackedDag::default();

    for method in input.methods {
        match method {
            DagMethod::Insert { node } => graph.insert(node),
            DagMethod::Delete { node } => graph.delete(node),
            DagMethod::Connect { v, u } => graph.connect(v, u),
            DagMethod::Disconnect { v, u } => graph.disconnect(v, u),
        }
    }
});
