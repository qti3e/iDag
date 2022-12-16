use fnv::{FnvHashMap, FnvHashSet};
use generational_arena::{Arena, Index};
use std::borrow::Borrow;
use std::collections::{hash_map, VecDeque};
use std::hash::Hash;
use std::ops::RangeBounds;

/// An incremental directed acyclic graph, however the word 'acyclic' is used, this structure allows
/// the existence of cycles in the hope that they will eventually be resolved and provides APIs to
/// report the cycles to the user of the structure.
pub struct Dag<N> {
    /// The metadata of each node.
    entries: Arena<Entry>,
    /// Match each node to the index of the arena.
    nodes: FnvHashMap<N, Index>,
    /// All of the cycles in this graph.
    cycles: FnvHashSet<Cycle>,
    /// The first order that is available to use.
    next_order: u64,
}

/// An entry in the DAG which contains the metadata about an edge.
#[derive(Default)]
struct Entry {
    forward: FnvHashSet<Index>,
    backward: FnvHashSet<Index>,
    order: u64,
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    SelfLoop,
}

/// A helper structure that keeps track of a visit stack and the set of already visited nodes and
/// visit the nodes in the graph in the given direction.
struct DagTraverser {
    /// The nodes that we should visit.
    stack: VecDeque<Index>,
    /// Nodes that we have already visited.
    visited: FnvHashSet<Index>,
    /// The direction in which we should be moving.
    direction: Direction,
}

/// The direction at which we should move to find.
enum Direction {
    /// Move the traverser in the forward direction and visit the children.
    Forward,
    /// Move the traverser in the backward direction and visit the parents.
    Backward,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum ControlFlow {
    /// Stop the traversal immediately.
    Stop,
    /// Continue as normal.
    Continue,
    /// Skip visiting the children.
    SkipChildren,
}

/// A visitor that stops when it finds a target node.
struct SearchVisitor {
    target: Index,
    found: bool,
}

/// A visitor that just collects all of the nodes that it sees.
#[derive(Default)]
struct CollectVisitor {
    collected: Vec<(Index, u64)>,
}

trait Visitor {
    fn visit(&mut self, index: &Index, order: u64) -> ControlFlow;
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct Cycle(Index, Index);

enum GraphChange {
    DeleteNode(Index),
    DeleteEdge(Index, Index),
}

impl<N> Dag<N>
where
    N: Hash + Eq,
{
    /// Create a new empty DAG instance.
    pub fn new() -> Self {
        Self {
            entries: Arena::new(),
            nodes: FnvHashMap::default(),
            cycles: FnvHashSet::default(),
            next_order: 0,
        }
    }

    /// Insert a new node to the graph.
    pub fn insert(&mut self, node: N) {
        if let hash_map::Entry::Vacant(e) = self.nodes.entry(node) {
            let entry = Entry {
                order: self.next_order,
                ..Entry::default()
            };
            let index = self.entries.insert(entry);
            e.insert(index);
            self.next_order += 1;
        }
    }

    /// Remove a node from the graph.
    pub fn remove<Q: ?Sized>(&mut self, node: &Q) -> Option<N>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let (node, index) = if let Some((n, index)) = self.nodes.remove_entry(node) {
            (n, index)
        } else {
            return None;
        };

        let entry = self.entries.remove(index).unwrap();

        for i in entry.forward {
            let entry = self.entries.get_mut(i).unwrap();
            entry.backward.remove(&index);
        }

        for i in entry.backward {
            let entry = self.entries.get_mut(i).unwrap();
            entry.forward.remove(&index);
        }

        if !self.cycles.is_empty() {
            self.update_cycles(GraphChange::DeleteNode(index));
        }

        Some(node)
    }

    //// Insert a new edge to the graph.
    pub fn connect<Q: ?Sized>(&mut self, v: &Q, u: &Q) -> Result<bool, Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = *self.nodes.get(v).ok_or(Error::NotFound)?;
        let u_index = *self.nodes.get(u).ok_or(Error::NotFound)?;

        // self loops are not allowed.
        if v_index == u_index {
            return Err(Error::SelfLoop);
        }

        // insert the edge.
        let (v_order, u_order) = {
            let tmp = self.entries.get2_mut(v_index, u_index);
            let v_entry = tmp.0.unwrap();
            let u_entry = tmp.1.unwrap();
            if !v_entry.forward.insert(u_index) {
                // the connection already exists.
                return Ok(false);
            }
            u_entry.backward.insert(v_index);
            (v_entry.order, u_entry.order)
        };

        if v_order > u_order {
            self.add_edge_helper(v_index, u_index, false);
        }

        Ok(true)
    }

    /// Removes an edge from the graph.
    pub fn disconnect<Q: ?Sized>(&mut self, v: &Q, u: &Q) -> Result<bool, Error>
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let v_index = self.nodes.get(v).ok_or(Error::NotFound)?;
        let u_index = self.nodes.get(u).ok_or(Error::NotFound)?;

        if v_index == u_index {
            return Err(Error::SelfLoop);
        }

        {
            let tmp = self.entries.get2_mut(*v_index, *u_index);
            let v_entry = tmp.0.unwrap();
            let u_entry = tmp.1.unwrap();

            if !v_entry.forward.remove(u_index) {
                // the connection does not even exists.
                return Ok(false);
            }

            u_entry.backward.remove(v_index);
        }

        if !self.cycles.is_empty() {
            self.update_cycles(GraphChange::DeleteEdge(*v_index, *u_index));
        }

        Ok(true)
    }

    /// Returns `true` if the graph contains the given node.
    pub fn contains<Q: ?Sized>(&self, v: &Q) -> bool
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.nodes.contains_key(v)
    }

    /// Returns `true` if there is a direct edge connection `v` to `u`.
    pub fn is_connected<Q: ?Sized>(&self, v: &Q, u: &Q) -> bool
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let (v_index, u_index) = match (self.nodes.get(v), self.nodes.get(u)) {
            (Some(v_index), Some(u_index)) => (v_index, u_index),
            _ => return false,
        };
        // check the existence of the edge backward.
        self.entries
            .get(*u_index)
            .unwrap()
            .backward
            .contains(v_index)
    }

    /// Returns `true` if there is a path from `v` to `u`.
    pub fn is_reachable<Q: ?Sized>(&self, v: &Q, u: &Q) -> bool
    where
        N: Borrow<Q>,
        Q: Hash + Eq,
    {
        let (v_index, u_index) = match (self.nodes.get(v), self.nodes.get(u)) {
            (Some(v_index), Some(u_index)) => (v_index, u_index),
            _ => return false,
        };
        let v_entry = self.entries.get(*v_index).unwrap();
        let u_entry = self.entries.get(*u_index).unwrap();
        // construct the traverser that searches for `u`.
        let mut visitor = SearchVisitor::new(*u_index);
        let mut traverser = DagTraverser::new(Direction::Forward);
        traverser.push_index(*v_index);
        // return the result based on the searches.
        if v_entry.order < u_entry.order {
            traverser.traverse(&self, 0..=u_entry.order, &mut visitor);
            visitor.found
        } else if !self.cycles.is_empty() {
            // If there is no cycle in this graph, then there is not gonna be a path from v->u.
            false
        } else {
            traverser.traverse(&self, 0..=u64::MAX, &mut visitor);
            visitor.found
        }
    }

    #[inline(always)]
    fn update_cycles(&mut self, change: GraphChange) {
        // The strategy is simple:
        // 1. Remove every cycle that is immediately effected by this change.
        // 2. Iterate over every cycle and check if they are resolved.
        // 3. If so, remove the cycle and attend inserting it again to reorder it.

        let cycles = std::mem::take(&mut self.cycles);

        for cycle in cycles {
            if change.should_remove(&cycle) {
                // the cycle should immediately be removed. There is nothing else to do.
                continue;
            }

            let v = cycle.0;
            let u = cycle.0;

            // check for the existence of v -> u.
            // the add_edge_helper function checks the existence of u -> v, and if it exits then
            // it will insert a new cycle to self.cycles.
            let mut visitor = SearchVisitor::new(u);
            let mut traverser = DagTraverser::new(Direction::Forward);
            traverser.push_index(v);
            traverser.traverse(&self, 0..=u64::MAX, &mut visitor);

            if !visitor.found {
                // The cycle is resolved so we can move on.
                continue;
            }

            self.add_edge_helper(v, u, true);
        }
    }

    /// Performs the necessary reordering upon insertion of a new edge, it is also called from
    /// update_cycles for when a cycle is resolved.
    fn add_edge_helper(&mut self, v_index: Index, u_index: Index, visit_all: bool) {
        let mut traverser = DagTraverser::new(Direction::Forward);
        let mut visited_forward = CollectVisitor::default();
        let mut visited_backward = CollectVisitor::default();

        let (v_order, u_order) = {
            let v_entry = self.entries.get(v_index).unwrap();
            let u_entry = self.entries.get(u_index).unwrap();
            (v_entry.order, u_entry.order)
        };

        let range = if self.cycles.is_empty() && !visit_all {
            0..=v_order
        } else {
            0..=u64::MAX
        };

        // Start from `u` and move forward, here we want to see if there is a path from u -> v, and
        // in that case we have found a cycle.
        traverser.push_index(u_index);
        traverser.traverse(self, range, &mut visited_forward);

        if traverser.has_visited(&v_index) {
            // We have found a cycle. So we should report it and keep track of it.
            self.cycles.insert(Cycle(v_index, u_index));
        } else {
            // Reorder the graph to maintain the topological ordering.
            traverser.move_backward();
            traverser.push_index(v_index);
            traverser.traverse(self, (u_order + 1).., &mut visited_backward);
            let visited_forward = visited_forward.collected;
            let visited_backward = visited_backward.collected;
            self.reorder(visited_forward, visited_backward);
        }
    }

    fn reorder(
        &mut self,
        mut visited_forward: Vec<(Index, u64)>,
        mut visited_backward: Vec<(Index, u64)>,
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
            self.entries.get_mut(index).unwrap().order = if o1 < o2 {
                i1 += 1;
                o1
            } else {
                i2 += 1;
                o2
            };
        }

        while i1 < len1 {
            let index = index_iter.next().unwrap().0;
            self.entries.get_mut(index).unwrap().order = visited_forward[i1].1;
            i1 += 1;
        }

        while i2 < len2 {
            let index = index_iter.next().unwrap().0;
            self.entries.get_mut(index).unwrap().order = visited_backward[i2].1;
            i2 += 1;
        }
    }
}

impl DagTraverser {
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
    pub fn has_visited(&self, node: &Index) -> bool {
        self.visited.contains(node)
    }

    /// Push a new node to the traverser stack so it can be visited.
    #[inline(always)]
    pub fn push_index(&mut self, index: Index) {
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
            let entry = dag.entries.get(index).unwrap();

            // check if the node is within the range of our traversal.
            if !range.contains(&entry.order) {
                continue;
            }

            // only visit once.
            if !self.visited.insert(index) {
                continue;
            }

            // pass it to the visitor.
            match visitor.visit(&index, entry.order) {
                ControlFlow::Continue => {}
                ControlFlow::Stop => break,
                ControlFlow::SkipChildren => continue,
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
    pub fn new(target: Index) -> Self {
        SearchVisitor {
            target,
            found: false,
        }
    }
}

impl Visitor for SearchVisitor {
    #[inline(always)]
    fn visit(&mut self, index: &Index, _order: u64) -> ControlFlow {
        if self.found || &self.target == index {
            self.found = true;
            ControlFlow::Stop
        } else {
            ControlFlow::Continue
        }
    }
}

impl Visitor for CollectVisitor {
    #[inline(always)]
    fn visit(&mut self, index: &Index, order: u64) -> ControlFlow {
        self.collected.push((*index, order));
        ControlFlow::Continue
    }
}

impl Visitor for () {
    #[inline(always)]
    fn visit(&mut self, _index: &Index, _order: u64) -> ControlFlow {
        ControlFlow::Continue
    }
}

impl<N> Default for Dag<N>
where
    N: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl GraphChange {
    /// Returns `true` if the cycle should be removed based on this change.
    #[inline(always)]
    pub fn should_remove(&self, cycle: &Cycle) -> bool {
        match self {
            Self::DeleteEdge(v, u) => &cycle.0 == v && &cycle.1 == u,
            Self::DeleteNode(v) => &cycle.0 == v || &cycle.1 == v,
        }
    }
}
