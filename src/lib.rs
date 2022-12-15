use fnv::{FnvHashMap, FnvHashSet};
use generational_arena::{Arena, Index};
use std::borrow::Borrow;
use std::collections::{hash_map, HashMap, VecDeque};
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
    pub fn with_capacity(_n: u64) -> Self {
        todo!()
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

        let cycles = {
            let tmp = self.entries.get2_mut(v_index.0, u_index.0);
            let v_entry = tmp.0.unwrap();
            let u_entry = tmp.1.unwrap();

            if !v_entry.forward.remove(u_index) {
                // the connection does not even exists.
                return Ok(());
            }

            u_entry.backward.insert(*v_index);

            v_entry
                .cycles
                .intersection(&u_entry.cycles)
                .cloned()
                .collect::<Vec<_>>()
        };

        if !cycles.is_empty() {}

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
        let u_entry = self.entries.get(u_index.0).unwrap();
        let mut visitor = SearchVisitor::new(u_index);
        let mut traverser = Traverser::new(Direction::Forward);
        traverser.push_index(v_index);
        traverser.traverse(&self, 0..u_entry.order, &mut visitor);
        Ok(visitor.finish())
    }

    fn update_cycles(&mut self, reason: UpdateCyclesReason, cycles: FnvHashSet<CycleIndex>) {
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
            traverser.traverse(&self, 0..=u64::MAX, &mut ());
            for index in &traverser.visited {
                let entry = self.entries.get_mut(index.0).unwrap();
                for c in &to_be_removed {
                    entry.cycles.remove(c);
                }
            }
        }

        for (index, Cycle(v_index, u_index)) in to_check {
            let mut traverser = Traverser::new(Direction::Forward);
            traverser.push_index(u_index);
            traverser.traverse(&self, 0..=u64::MAX, &mut ());

            let reorder = if traverser.has_visited(&v_index) {
                // The cycle is not destroyed.
                let mut visited_forward = CollectVisitor::default();
                traverser.push_index(*reason.base());
                traverser.traverse(&self, 0..=u64::MAX, &mut visited_forward);
                false
            } else {
                // The cycle is destroyed.
                self.cycles.remove(index.0);
                traverser.push_index(*reason.base());
                traverser.push_index(v_index);
                traverser.traverse(&self, 0..=u64::MAX, &mut ());
                true
            };

            for forward in traverser.visited {
                let entry = self.entries.get_mut(forward.0).unwrap();
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
        let (cycles, v_order, u_order) = {
            let v_entry = self.entries.get(v_index.0).unwrap();
            let u_entry = self.entries.get(u_index.0).unwrap();

            let cycles = v_entry
                .cycles
                .difference(&u_entry.cycles)
                .cloned()
                .collect::<Vec<_>>();

            (cycles, v_entry.order, u_entry.order)
        };

        // if any such cycle is found, insert those to `u` and all of `u`'s children.
        if !cycles.is_empty() {
            traverser.push_index(u_index);
            traverser.traverse(&self, 0..=u64::MAX, &mut visited_forward);

            for (node_index, _) in &mut visited_forward.visited {
                self.entries
                    .get_mut(node_index.0)
                    .unwrap()
                    .cycles
                    .extend(cycles.iter().copied());
            }
        }

        // if we're already sorted, we don't need to make any changes.
        if v_order <= u_order {
            return;
        }

        if !traverser.has_visited(&u_index) {
            // otherwise we have already visited all of the children of `u`.
            traverser.push_index(u_index);
            traverser.traverse(&self, 0..=v_order, &mut visited_forward);
        }

        if traverser.has_visited(&v_index) {
            // We have found a cycle, and now we want to mark all of the children of `v` as part
            // of the new cycle.
            // For that, we first visit all of the nodes starting from `v`.
            traverser.push_index(v_index);
            traverser.traverse(&self, 0..=u64::MAX, &mut visited_forward);

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
            traverser.traverse(&self, u_order.., &mut visited_backward);
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
        self.direction = Direction::Forward;
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

            // only pass the node to the visitor if it has not been seen before.
            if self.visited.insert(index) {
                // pass it to the visitor.
                if visitor.visit(&index, entry.order) == ControlFlow::Break(()) {
                    break;
                }
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

#[test]
fn x() {
    let mut dag = Dag::<String>::new();
    dag.insert("B".into());
    dag.insert("A".into());
    dag.insert("C".into());
    dag.insert("D".into());
    dag.connect("B", "A").unwrap();
    dag.connect("A", "B").unwrap();
    dag.connect("A", "C").unwrap();
    // dag.connect("D", "C").unwrap();
    // dag.connect("C", "A").unwrap();
    dag.remove("C");

    let mut nodes = dag.nodes.iter().collect::<Vec<_>>();
    nodes.sort_by_key(|(_, i)| dag.entries.get(i.0).unwrap().order);
    let nodes = nodes.into_iter().map(|a| a.0).collect::<Vec<_>>();

    println!("Order=");
    println!("{:?}", nodes);
    println!("Cycles=");
    {
        let mut index_to_str = HashMap::new();
        let mut cycles = Vec::<(&String, &String)>::new();
        for (s, i) in dag.nodes {
            index_to_str.insert(i, s);
        }
        for cycle in dag.cycles {
            let v = index_to_str.get(&cycle.0).unwrap();
            let u = index_to_str.get(&cycle.1).unwrap();
            cycles.push((v, u));
        }
        println!("{:?}", cycles);
    }
}
