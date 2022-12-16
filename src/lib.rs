mod impl2;

use fnv::{FnvBuildHasher, FnvHashMap, FnvHashSet};
use generational_arena::{Arena, Index};
use std::borrow::Borrow;
use std::collections::{hash_map, VecDeque};
use std::hash::Hash;
use std::ops::{ControlFlow, RangeBounds};

pub struct Dag<N> {
    /// The metadata for each node.
    entries: Arena<Entry>,
    /// The circles found in this graph.
    cycles: Arena<Cycle>,
    /// All of the nodes in the graph.
    nodes: FnvHashMap<N, NodeIndex>,
    /// The next free order.
    next_order: u64,
}

#[derive(Debug)]
pub enum Error {
    /// The requested node was not found on the graph.
    NodeNotFound,
    SelfLoop,
}

#[derive(Default)]
struct Entry {
    /// Children of this node.
    forward: FnvHashSet<NodeIndex>,
    /// Parents of this node.
    backward: FnvHashSet<NodeIndex>,
    /// The position of this node in the topological ordering.
    order: u64,
    /// The cycles this node belongs to.
    cycles: FnvHashSet<CycleIndex>,
}

/// The reason triggering the request to update cycles.
enum UpdateCyclesReason {
    DeleteEdge(NodeIndex, NodeIndex),
    DeleteNode(NodeIndex),
}

/// A cycle is stored as an edge that introduced the cycle for the first time.
#[derive(Copy, Clone)]
struct Cycle(NodeIndex, NodeIndex);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeIndex(Index);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CycleIndex(Index);

struct Traverser {
    direction: Direction,
    stack: VecDeque<NodeIndex>,
    visited: FnvHashSet<NodeIndex>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Direction {
    Forward,
    Backward,
}

trait Visitor {
    type Result;

    fn visit(&mut self, index: &NodeIndex, order: u64) -> ControlFlow<()>;

    fn finish(self) -> Self::Result;
}

/// A visitor that searches for an element.
struct SearchVisitor {
    target: NodeIndex,
    found: bool,
}

/// A visitor that stores the order at which nodes were visited.
#[derive(Default)]
struct CollectVisitor {
    visited: Vec<(NodeIndex, u64)>,
}

impl<N> Dag<N>
where
    N: Eq + Hash,
{
    /// Create a new empty dag with no nodes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new empty dag with pre-allocated storage for `n` nodes.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            entries: Arena::with_capacity(n),
            cycles: Arena::with_capacity(n),
            nodes: FnvHashMap::with_capacity_and_hasher(n, FnvBuildHasher::default()),
            next_order: 0,
        }
    }

    /// Insert a new node into the graph. Performs nothing if the nodes is already inserted.
    pub fn insert(&mut self, v: N) {
        if let hash_map::Entry::Vacant(e) = self.nodes.entry(v) {
            let entry = Entry {
                order: self.next_order,
                ..Entry::default()
            };
            self.next_order += 1;
            let index = self.entries.insert(entry);
            e.insert(NodeIndex(index));
        }
    }

    /// Remove a node from the graph.
    pub fn remove<Q: ?Sized>(&mut self, v: &Q)
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let index = if let Some(index) = self.nodes.remove(v) {
            index
        } else {
            return;
        };

        let (backward, cycles) = {
            let entry = self.entries.get_mut(index.0).unwrap();
            (
                std::mem::take(&mut entry.backward),
                std::mem::take(&mut entry.cycles),
            )
        };

        // Delete all of the forward links to this node.
        for u_index in backward {
            let u_entry = self.entries.get_mut(u_index.0).unwrap();
            u_entry.forward.remove(&index);
        }

        if !cycles.is_empty() {
            self.update_cycles(UpdateCyclesReason::DeleteNode(index), cycles);
        }

        let entry = self.entries.remove(index.0).unwrap();

        // Delete all of backward links to this node.
        for u_index in entry.forward {
            let u_entry = self.entries.get_mut(u_index.0).unwrap();
            u_entry.backward.remove(&index);
        }
    }

    /// Create a connection in the graph between two nodes.
    pub fn connect<Q: ?Sized>(&mut self, v: &Q, u: &Q) -> Result<(), Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = *self.nodes.get(v).ok_or(Error::NodeNotFound)?;
        let u_index = *self.nodes.get(u).ok_or(Error::NodeNotFound)?;

        if v_index == u_index {
            return Err(Error::SelfLoop);
        }

        if self
            .entries
            .get(v_index.0)
            .unwrap()
            .forward
            .contains(&u_index)
        {
            return Ok(());
        }

        self.add_edge_helper(v_index, u_index);

        self.entries
            .get_mut(v_index.0)
            .unwrap()
            .forward
            .insert(u_index);

        self.entries
            .get_mut(u_index.0)
            .unwrap()
            .backward
            .insert(v_index);

        Ok(())
    }

    /// Remove a connection from the graph between two nodes.
    pub fn disconnect<Q: ?Sized>(&mut self, v: &Q, u: &Q) -> Result<(), Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = self.nodes.get(v).ok_or(Error::NodeNotFound)?;
        let u_index = self.nodes.get(u).ok_or(Error::NodeNotFound)?;

        if v_index == u_index {
            return Err(Error::SelfLoop);
        }

        let cycles = {
            let tmp = self.entries.get2_mut(v_index.0, u_index.0);
            let v_entry = tmp.0.unwrap();
            let u_entry = tmp.1.unwrap();

            if !v_entry.forward.remove(u_index) {
                // the connection does not even exists.
                return Ok(());
            }

            u_entry.backward.remove(v_index);

            v_entry
                .cycles
                .intersection(&u_entry.cycles)
                .cloned()
                .collect::<FnvHashSet<_>>()
        };

        if !cycles.is_empty() {
            self.update_cycles(UpdateCyclesReason::DeleteEdge(*v_index, *u_index), cycles);
        }

        Ok(())
    }

    pub fn is_connected<Q: ?Sized>(&self, v: &Q, u: &Q) -> Result<bool, Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = self.nodes.get(v).ok_or(Error::NodeNotFound)?;
        let u_index = self.nodes.get(u).ok_or(Error::NodeNotFound)?;
        let v_entry = self.entries.get(v_index.0).unwrap();
        Ok(v_entry.forward.contains(u_index))
    }

    /// Returns true if the node `u` is reachable starting from node `v`.
    pub fn is_reachable<Q: ?Sized>(&self, v: &Q, u: &Q) -> Result<bool, Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = *self.nodes.get(v).ok_or(Error::NodeNotFound)?;
        let u_index = *self.nodes.get(u).ok_or(Error::NodeNotFound)?;
        let v_entry = self.entries.get(v_index.0).unwrap();
        let u_entry = self.entries.get(u_index.0).unwrap();
        let has_cycles = !v_entry.cycles.is_empty() || !u_entry.cycles.is_empty();

        if v_entry.order > u_entry.order {
            if !has_cycles {
                return Ok(false);
            }

            let mut visitor = SearchVisitor::new(v_index);
            let mut traverser = Traverser::new(Direction::Backward);
            traverser.push_index(u_index);
            traverser.traverse(self, 0..=u64::MAX, &mut visitor);
            return Ok(visitor.finish());
        }

        let range = if has_cycles {
            0..=u64::MAX
        } else {
            0..=u_entry.order
        };

        let mut visitor = SearchVisitor::new(u_index);
        let mut traverser = Traverser::new(Direction::Forward);
        traverser.push_index(v_index);
        traverser.traverse(self, range, &mut visitor);
        Ok(visitor.finish())
    }

    /// Return the order of a given node in this DAG.
    pub fn get_order<Q: ?Sized>(&self, v: &Q) -> Option<u64>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = *self.nodes.get(v).ok_or(Error::NodeNotFound).unwrap();
        let v_entry = self.entries.get(v_index.0).unwrap();
        if v_entry.cycles.is_empty() {
            Some(v_entry.order)
        } else {
            None
        }
    }

    fn update_cycles(&mut self, reason: UpdateCyclesReason, cycles: FnvHashSet<CycleIndex>) {
        debug_assert!(!cycles.is_empty());

        let mut to_be_removed = Vec::<CycleIndex>::new();
        let mut to_check = Vec::<(CycleIndex, Cycle)>::new();
        let mut traverser = Traverser::new(Direction::Forward);

        for index in cycles {
            let cycle = self.cycles.get(index.0).unwrap();
            if reason.should_remove(cycle) {
                to_be_removed.push(index);
                traverser.push_index(cycle.0);
                traverser.push_index(cycle.1);
            } else {
                to_check.push((index, *cycle));
            }
        }

        if !to_be_removed.is_empty() {
            traverser.traverse(self, 0..=u64::MAX, &mut ());
            for index in &traverser.visited {
                let entry = self.entries.get_mut(index.0).unwrap();
                for c in &to_be_removed {
                    entry.cycles.remove(c);
                }
            }
            for c in &to_be_removed {
                self.cycles.remove(c.0);
            }
        }

        let mut traverser = Traverser::new(Direction::Forward);

        for (index, Cycle(v_index, u_index)) in to_check {
            traverser.visited.clear();

            let mut visited_forward = CollectVisitor::default();
            traverser.push_index(u_index);
            traverser.traverse(self, 0..=u64::MAX, &mut visited_forward);

            let reorder = if traverser.has_visited(&v_index) {
                // The cycle is not destroyed, we should delete the cycle only from the elements
                // that are not in the cycle anymore.
                visited_forward.clear();
                traverser.push_index(*reason.base());
                traverser.traverse(&self, 0..=u64::MAX, &mut visited_forward);
                false
            } else {
                // The cycle is destroyed.
                self.cycles.remove(index.0);
                match reason {
                    UpdateCyclesReason::DeleteEdge(a, b) => {
                        traverser.push_index(a);
                        traverser.push_index(b);
                    }
                    UpdateCyclesReason::DeleteNode(a) => {
                        traverser.push_index(a);
                    }
                }
                traverser.push_index(v_index);
                traverser.traverse(self, 0..=u64::MAX, &mut visited_forward);
                true
            };

            for (node_index, _) in visited_forward.finish() {
                let entry = self.entries.get_mut(node_index.0).unwrap();
                entry.cycles.remove(&index);
            }

            if reorder {
                self.add_edge_helper(v_index, u_index);
            }
        }
    }

    fn add_edge_helper(&mut self, v_index: NodeIndex, u_index: NodeIndex) {
        let mut traverser = Traverser::new(Direction::Forward);
        let mut visited_forward = CollectVisitor::default();
        let mut visited_backward = CollectVisitor::default();

        // compute the cycles that are present in `v` but not in `u`.
        let (cycles, v_order, u_order, has_cycles) = {
            let v_entry = self.entries.get(v_index.0).unwrap();
            let u_entry = self.entries.get(u_index.0).unwrap();

            let cycles = v_entry
                .cycles
                .difference(&u_entry.cycles)
                .cloned()
                .collect::<Vec<_>>();

            (
                cycles,
                v_entry.order,
                u_entry.order,
                !v_entry.cycles.is_empty() || !u_entry.cycles.is_empty(),
            )
        };

        // if any such cycle is found, insert those to `u` and all of `u`'s children.
        if !cycles.is_empty() {
            traverser.push_index(u_index);
            traverser.traverse(self, 0..=u64::MAX, &mut visited_forward);

            for (node_index, _) in &mut visited_forward.visited {
                self.entries
                    .get_mut(node_index.0)
                    .unwrap()
                    .cycles
                    .extend(cycles.iter().copied());
            }
        }

        // if we're already sorted, we don't need to make any changes.
        if v_order <= u_order && !has_cycles {
            return;
        }

        if !traverser.has_visited(&u_index) {
            let range = if v_order <= u_order {
                0..=u64::MAX
            } else {
                0..=v_order
            };
            // otherwise we have already visited all of the children of `u`.
            traverser.push_index(u_index);
            traverser.traverse(self, range, &mut visited_forward);
        }

        if traverser.has_visited(&v_index) {
            // We have found a cycle, and now we want to mark all of the children of `v` as part
            // of the new cycle.
            // For that, we first visit all of the nodes starting from `v`.
            traverser.push_index(v_index);
            traverser.traverse(self, 0..=u64::MAX, &mut visited_forward);

            // create the new cycle.
            let cycle = Cycle(v_index, u_index);
            let cycle_index = CycleIndex(self.cycles.insert(cycle));

            for (node_index, _) in &mut visited_forward.visited {
                self.entries
                    .get_mut(node_index.0)
                    .unwrap()
                    .cycles
                    .insert(cycle_index);
            }
        } else {
            traverser.move_backward();
            traverser.push_index(v_index);
            traverser.traverse(self, (u_order + 1).., &mut visited_backward);
            let visited_forward = visited_forward.finish();
            let visited_backward = visited_backward.finish();
            self.reorder(visited_forward, visited_backward);
        }
    }

    fn reorder(
        &mut self,
        mut visited_forward: Vec<(NodeIndex, u64)>,
        mut visited_backward: Vec<(NodeIndex, u64)>,
    ) {
        // sort the nodes by their original order.
        visited_forward.sort_by_key(|(_, order)| *order);
        visited_backward.sort_by_key(|(_, order)| *order);

        let len1 = visited_forward.len();
        let len2 = visited_backward.len();
        let mut i1 = 0usize;
        let mut i2 = 0usize;
        let mut index_iter = visited_backward.iter().chain(visited_forward.iter());

        while i1 < len1 && i2 < len2 {
            let (_, o1) = visited_forward[i1];
            let (_, o2) = visited_backward[i2];

            let index = index_iter.next().unwrap().0;
            self.entries.get_mut(index.0).unwrap().order = if o1 < o2 {
                i1 += 1;
                o1
            } else {
                i2 += 1;
                o2
            };
        }

        while i1 < len1 {
            let index = index_iter.next().unwrap().0;
            self.entries.get_mut(index.0).unwrap().order = visited_forward[i1].1;
            i1 += 1;
        }

        while i2 < len2 {
            let index = index_iter.next().unwrap().0;
            self.entries.get_mut(index.0).unwrap().order = visited_backward[i2].1;
            i2 += 1;
        }
    }
}

impl Traverser {
    /// Create a new traverser that moves in the given direction.
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            stack: VecDeque::new(),
            visited: FnvHashSet::default(),
        }
    }

    /// Returns true if the given element is visited.
    #[inline(always)]
    pub fn has_visited(&self, node: &NodeIndex) -> bool {
        self.visited.contains(node)
    }

    /// Push a new node to the traverser stack so it can be visited.
    #[inline(always)]
    pub fn push_index(&mut self, index: NodeIndex) {
        // do not check if the node is visited since this can be a strategy when the controller
        // wants to traverse the graph forward and backward.
        self.stack.push_front(index);
    }

    /// Change the traverse's direction to move forward.
    #[inline(always)]
    pub fn move_forward(&mut self) {
        self.direction = Direction::Forward;
    }

    /// Change the traverse's direction to move backward.
    #[inline(always)]
    pub fn move_backward(&mut self) {
        self.direction = Direction::Backward;
    }

    /// Start visiting the provided graph at the given range with the provided visitor.
    pub fn traverse<N, R: RangeBounds<u64>, V: Visitor>(
        &mut self,
        dag: &Dag<N>,
        range: R,
        visitor: &mut V,
    ) {
        while let Some(index) = self.stack.pop_back() {
            let entry = dag.entries.get(index.0).unwrap();

            // check if the node is within the range of our traversal.
            if !range.contains(&entry.order) {
                continue;
            }

            // only visit once.
            if !self.visited.insert(index) {
                continue;
            }

            // pass it to the visitor.
            if visitor.visit(&index, entry.order) == ControlFlow::Break(()) {
                break;
            }

            // get the next nodes we have to visit depending on the direction.
            let to_visit = match self.direction {
                Direction::Forward => &entry.forward,
                Direction::Backward => &entry.backward,
            };

            // count the new items we need to append to the stack.
            let mut new_items = 0;
            for v_index in to_visit {
                if self.visited.contains(v_index) {
                    // the node is already visited - ignore it.
                    continue;
                }

                new_items += 1;
            }

            // insert the items to the stack so we can visit them later.
            self.stack.reserve(new_items);

            // perform the insertion of items to the stack.
            for v_index in to_visit {
                if self.visited.contains(v_index) {
                    continue;
                }

                self.stack.push_front(*v_index);
            }
        }
    }
}

impl SearchVisitor {
    /// Create a new traverser that searches for the given target node.
    #[inline(always)]
    pub fn new(target: NodeIndex) -> Self {
        Self {
            target,
            found: false,
        }
    }
}

impl Visitor for SearchVisitor {
    type Result = bool;

    #[inline(always)]
    fn visit(&mut self, index: &NodeIndex, _order: u64) -> ControlFlow<()> {
        if self.found || &self.target == index {
            self.found = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    #[inline(always)]
    fn finish(self) -> Self::Result {
        self.found
    }
}

impl Visitor for CollectVisitor {
    type Result = Vec<(NodeIndex, u64)>;

    fn visit(&mut self, index: &NodeIndex, order: u64) -> ControlFlow<()> {
        self.visited.push((*index, order));
        ControlFlow::Continue(())
    }

    fn finish(self) -> Self::Result {
        self.visited
    }
}

impl CollectVisitor {
    #[inline(always)]
    pub fn clear(&mut self) {
        self.visited.clear();
    }
}

impl Visitor for () {
    type Result = ();

    fn visit(&mut self, _index: &NodeIndex, _order: u64) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }

    fn finish(self) -> Self::Result {}
}

impl<N> Default for Dag<N> {
    fn default() -> Self {
        Dag {
            entries: Default::default(),
            cycles: Default::default(),
            nodes: FnvHashMap::default(),
            next_order: 0,
        }
    }
}

impl UpdateCyclesReason {
    /// Returns `true` if the cycle should be removed based on this change.
    #[inline]
    pub fn should_remove(&self, cycle: &Cycle) -> bool {
        match self {
            UpdateCyclesReason::DeleteEdge(v, u) => &cycle.0 == v && &cycle.1 == u,
            UpdateCyclesReason::DeleteNode(v) => &cycle.0 == v || &cycle.1 == v,
        }
    }

    /// Returns the `node` that is the base of the change.
    #[inline]
    pub fn base(&self) -> &NodeIndex {
        match self {
            UpdateCyclesReason::DeleteEdge(v, _) => v,
            UpdateCyclesReason::DeleteNode(v) => v,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils {
    use crate::{Cycle, Dag, NodeIndex};
    use fnv::{FnvHashMap, FnvHashSet};

    pub type Node = u8;

    impl Dag<Node> {
        /// Return the active cycles in this graph.
        fn cycles(&self) -> FnvHashSet<(Node, Node)> {
            let mut cycles = FnvHashSet::default();
            let mut index_to_node = FnvHashMap::<NodeIndex, Node>::default();

            for (node, index) in &self.nodes {
                index_to_node.insert(*index, *node);
            }

            for (_, Cycle(v, u)) in self.cycles.iter() {
                let v = *index_to_node.get(v).unwrap();
                let u = *index_to_node.get(u).unwrap();
                cycles.insert((v, u));
            }

            cycles
        }

        /// For every node in this graph, check if all the cycles exist.
        fn debug_check_cycles_exist(&self) {
            for (_, entry) in &self.entries {
                for cycle in &entry.cycles {
                    self.cycles.get(cycle.0).expect("Dead reference");
                }
            }
        }
    }

    /// The implementation wrapped with a naive implementation as a reference and checks are done
    /// per mutation on the graph to ensure the correctness of the incremental implementation.
    #[derive(Default)]
    pub struct NaiveBackedDag {
        nodes: FnvHashMap<Node, FnvHashSet<Node>>,
        cycles: FnvHashSet<(Node, Node)>,
        dag: Dag<Node>,
    }

    impl NaiveBackedDag {
        pub fn insert(&mut self, node: Node) {
            self.dag.insert(node);
            self.nodes.entry(node).or_default();
            self.assert_order();
        }

        pub fn delete(&mut self, node: Node) {
            self.dag.remove(&node);
            self.nodes.remove(&node);
            self.nodes.iter_mut().for_each(|(_, s)| {
                s.remove(&node);
            });
            self.update_cycles();
            self.assert_order();
        }

        pub fn connect(&mut self, v: Node, u: Node) {
            if v == u {
                self.dag.connect(&v, &u);
                self.update_cycles();
                self.assert_order();
                return;
            }

            if self.nodes.contains_key(&u) {
                if let Some(e) = self.nodes.get_mut(&v) {
                    // check if the connection doesn't already exist.
                    if e.insert(u) {
                        // if there is a path from `u -> v`, then we have a new cycle. but there
                        // shouldn't already be a path from `v -> u` otherwise the cycle is not
                        // new.
                        if self.dag.is_reachable(&u, &v).unwrap() {
                            self.cycles.insert((v, u));
                        }
                    }

                    self.dag.connect(&v, &u).unwrap();
                    self.assert_order();
                    return;
                }
            }
            assert!(self.dag.connect(&v, &u).is_err());
        }

        pub fn disconnect(&mut self, v: Node, u: Node) {
            if v == u {
                self.dag.disconnect(&v, &u);
                self.update_cycles();
                self.assert_order();
                return;
            }

            if self.nodes.contains_key(&u) {
                if let Some(e) = self.nodes.get_mut(&v) {
                    e.remove(&u);
                    self.dag.disconnect(&v, &u).unwrap();
                    self.cycles.retain(|(a, b)| !(a == &v && b == &u));
                    self.update_cycles();
                    self.assert_order();
                    return;
                }
            }
            assert!(self.dag.connect(&v, &u).is_err());
        }

        // iterate through all of the cycles and check if they still exits and remove the ones
        // that no longer exists from the graph.
        pub fn update_cycles(&mut self) {
            self.cycles.retain(|(v, u)| {
                if self.dag.nodes.contains_key(v) && self.dag.nodes.contains_key(u) {
                    self.dag.is_reachable(u, v).unwrap() && self.dag.is_reachable(v, u).unwrap()
                } else {
                    false
                }
            });
        }

        pub fn assert_order(&self) {
            self.dag.debug_check_cycles_exist();

            if !self.cycles.is_empty() || !self.dag.cycles.is_empty() {
                let actual_cycles = self.dag.cycles();
                assert_eq!(&actual_cycles, &self.cycles);
            }

            for (v, edges) in &self.nodes {
                for u in edges {
                    let v_order = self.dag.get_order(v);
                    let u_order = self.dag.get_order(u);
                    if let (Some(v_o), Some(u_o)) = (v_order, u_order) {
                        assert!(v_o < u_o)
                    }
                    assert!(
                        self.dag.is_connected(v, u).unwrap(),
                        "no connected: {} -> {}",
                        v,
                        u
                    );
                    assert!(
                        self.dag.is_reachable(v, u).unwrap(),
                        "not reachable: {} -> {}",
                        v,
                        u
                    );
                }
            }
        }

        pub fn is_connected(&self, v: Node, u: Node) -> bool {
            self.dag.is_connected(&v, &u).unwrap()
        }

        pub fn is_reachable(&self, v: Node, u: Node) -> bool {
            self.dag.is_reachable(&v, &u).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::NaiveBackedDag;

    #[test]
    fn e2e() {
        let mut g = NaiveBackedDag::default();
        g.insert(0);
        g.insert(1);
        g.insert(2);
        // A -> B
        g.connect(0, 1);
        // A -> B -> C
        g.connect(1, 2);
        // A -> B -> C
        // ^________/
        g.connect(2, 0);
        // +----+
        // V    |
        // A -> B -> C
        // ^_________+
        g.connect(1, 0);
        // A -> B -> C
        // ^_________+
        g.disconnect(1, 0);
        // B -> C -> A
        // \_________^
        g.disconnect(0, 1);
        g.connect(1, 0);
        // +----+
        // V    |
        // B -> C -> A
        // \_________^
        g.connect(2, 1);
        // +----+ +---+
        // V    | V   |
        // B ->  C -> A
        // \__________^
        g.connect(0, 2);
        // +----+
        // V    |
        // B -> C <- A
        // \_________^
        g.disconnect(2, 0);
        // B <-> C
        g.delete(0);
        // B -> C
        g.disconnect(2, 1);
    }

    #[test]
    fn x() {
        let mut g = NaiveBackedDag::default();
        g.insert(0);
        g.insert(177);
        g.insert(215);
        g.connect(0, 177);
        g.connect(177, 0);
        g.connect(0, 215);
        g.disconnect(0, 215);
        g.delete(177);
    }
}
