use super::Error;
use fnv::{FnvHashMap, FnvHashSet};
use generational_arena::Index;
use std::collections::{hash_map, VecDeque};

/// The implementation wrapped with a naive reference implementation, after every operation a series
/// of test is performed to check the correctness of the main implementation. This is used for our
/// fuzzing purposes.
#[derive(Default)]
pub struct WrappedDag {
    edges: FnvHashMap<Node, FnvHashSet<Node>>,
    dag: super::Dag<Node>,
    cycles: FnvHashSet<(Node, Node)>,
}

pub type Node = u8;

impl super::Dag<Node> {
    fn get_cycles(&self) -> FnvHashSet<(Node, Node)> {
        let index_to_node = self
            .nodes
            .iter()
            .map(|(k, v)| (*v, *k))
            .collect::<FnvHashMap<Index, Node>>();

        let mut cycles = FnvHashSet::default();
        cycles.reserve(self.cycles.len());

        for super::Cycle(v, u) in &self.cycles {
            let v = *index_to_node.get(v).unwrap();
            let u = *index_to_node.get(u).unwrap();
            cycles.insert((v, u));
        }

        cycles
    }

    fn get_order(&self, n: &Node) -> u64 {
        let index = self.nodes.get(n).unwrap();
        let entry = self.entries.get(*index).unwrap();
        entry.order
    }
}

impl WrappedDag {
    pub fn insert(&mut self, node: Node) {
        match self.edges.entry(node) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(FnvHashSet::default());
                assert!(self.dag.insert(node));
            }
            hash_map::Entry::Occupied(_) => {
                assert!(!self.dag.insert(node));
            }
        }
    }

    pub fn remove(&mut self, node: Node) {
        if self.edges.remove(&node).is_some() {
            assert!(self.dag.remove(&node).is_some());
        } else {
            assert!(self.dag.remove(&node).is_none());
            return;
        }

        for (_, set) in self.edges.iter_mut() {
            set.remove(&node);
        }

        self.cycles.retain(|(a, b)| a != &node && b != &node);
        self.update_cycles();
        assert_eq!(self.cycles, self.dag.get_cycles());
    }

    pub fn connect(&mut self, v: Node, u: Node) {
        let actual = self.dag.connect(&v, &u);
        let expected = if !self.edges.contains_key(&v) || !self.edges.contains_key(&u) {
            Err(Error::NotFound)
        } else if v == u {
            Err(Error::SelfLoop)
        } else {
            Ok(self.edges.get_mut(&v).unwrap().insert(u))
        };

        assert_eq!(actual, expected);

        if actual.is_ok() {
            assert!(self.dag.is_connected(&v, &u));
        }

        if actual == Ok(true) {
            if self.trusted_is_reachable(u, v) {
                self.cycles.insert((v, u));
            }

            assert_eq!(self.cycles, self.dag.get_cycles());

            if self.cycles.is_empty() {
                self.assert_topological_order();
            }
        }
    }

    pub fn disconnect(&mut self, v: Node, u: Node) {
        let actual = self.dag.disconnect(&v, &u);
        let expected = if !self.edges.contains_key(&v) || !self.edges.contains_key(&u) {
            Err(Error::NotFound)
        } else if v == u {
            Err(Error::SelfLoop)
        } else {
            Ok(self.edges.get_mut(&v).unwrap().remove(&u))
        };
        assert_eq!(actual, expected);

        if actual == Ok(true) {
            self.cycles.retain(|(a, b)| a != &v || b != &u);
            self.update_cycles();
            assert_eq!(self.cycles, self.dag.get_cycles());
        }
    }

    pub fn is_reachable(&self, v: Node, u: Node) {
        let actual = self.dag.is_reachable(&v, &u);
        let expected = self.trusted_is_reachable(v, u);
        assert_eq!(actual, expected);
    }

    fn trusted_is_reachable(&self, v: Node, u: Node) -> bool {
        if !self.edges.contains_key(&v) || !self.edges.contains_key(&u) {
            return false;
        }

        if v == u {
            return false;
        }

        let mut visited = FnvHashSet::default();
        let mut stack = VecDeque::<Node>::new();
        stack.push_front(v);

        while let Some(n) = stack.pop_back() {
            if n == u {
                return true;
            }

            if !visited.insert(n) {
                continue;
            }

            for c in self.edges.get(&n).unwrap() {
                stack.push_front(*c);
            }
        }

        false
    }

    /// Naive implementation for update_cycles which goes through every cycle and checks if it is
    /// still present or not.
    fn update_cycles(&mut self) {
        let cycles = std::mem::take(&mut self.cycles);

        for (v, u) in cycles {
            // A cycle is only present if for an edge `(v, u)` both v->u and u->v resolve.
            if self.trusted_is_reachable(v, u) && self.trusted_is_reachable(u, v) {
                self.cycles.insert((v, u));
            }
        }

        // If there is not a cycle, check the correctness of the topological ordering.
        if self.cycles.is_empty() {
            self.assert_topological_order();
        }
    }

    /// For every edge `(v, u)` in the graph check if `v` is coming before `u` in the ordering.
    fn assert_topological_order(&self) {
        assert!(self.cycles.is_empty() && self.dag.cycles.is_empty());
        for (v, set) in &self.edges {
            let v_order = self.dag.get_order(v);
            for u in set {
                let u_order = self.dag.get_order(u);
                assert!(
                    v_order < u_order,
                    "{:?}={} > {:?}={}",
                    v,
                    v_order,
                    u,
                    u_order
                );
            }
        }
    }
}

#[cfg(feature = "test-utils")]
pub mod fuzz {
    use super::*;
    use arbitrary::Arbitrary;
    use std::fmt::{Debug, Formatter};

    #[derive(Arbitrary)]
    pub struct Input {
        pub methods: Vec<DagMethod>,
    }

    #[derive(Arbitrary)]
    pub enum DagMethod {
        Insert { node: Node },
        Delete { node: Node },
        Connect { v: Node, u: Node },
        Disconnect { v: Node, u: Node },
        IsReachable { v: Node, u: Node },
    }

    impl Debug for DagMethod {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                DagMethod::Insert { node } => {
                    write!(f, "g.insert({:?});", node)
                }
                DagMethod::Delete { node } => {
                    write!(f, "g.remove({:?});", node)
                }
                DagMethod::Connect { v, u } => {
                    write!(f, "g.connect({:?}, {:?});", v, u)
                }
                DagMethod::Disconnect { v, u } => {
                    write!(f, "g.disconnect({:?}, {:?});", v, u)
                }
                DagMethod::IsReachable { v, u } => {
                    write!(f, "g.is_reachable({:?}, {:?});", v, u)
                }
            }
        }
    }

    impl Debug for Input {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut result = String::new();
            for method in &self.methods {
                result.push_str(&format!("{:?}", method));
                result.push('\n');
            }

            f.write_str(result.as_str())
        }
    }

    impl WrappedDag {
        #[inline(always)]
        pub fn run(&mut self, method: DagMethod) {
            match method {
                DagMethod::Insert { node } => self.insert(node),
                DagMethod::Delete { node } => self.remove(node),
                DagMethod::Connect { v, u } => self.connect(v, u),
                DagMethod::Disconnect { v, u } => self.disconnect(v, u),
                DagMethod::IsReachable { v, u } => self.is_reachable(v, u),
            }
        }
    }
}
