use super::Error;
use fnv::{FnvHashMap, FnvHashSet};
use generational_arena::Index;
use std::collections::{hash_map, VecDeque};

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

    fn update_cycles(&mut self) {
        let cycles = std::mem::take(&mut self.cycles);
        for (v, u) in cycles {
            if self.trusted_is_reachable(v, u) && self.trusted_is_reachable(u, v) {
                self.cycles.insert((v, u));
            }
        }

        if self.cycles.is_empty() {
            self.assert_topological_order();
        }
    }

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
