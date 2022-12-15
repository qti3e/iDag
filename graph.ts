import { PriorityQueue } from './priority_queue';
export type Cycle<V> = { readonly from: V; readonly to: V };

type CycleEdge<V> = [Entry<V>, Entry<V>, Cycle<V>];

/**
 * Meta data stored for each vertex.
 */
interface Entry<Vertex> {
  /**
   * The actual vertex.
   */
  vertex: Vertex;
  /**
   * Position of this vertex in the topologically ordered set of vertices.
   */
  order: number;
  /**
   * Temporary variable used in DFS.
   */
  visited: boolean;
  /**
   * Whether this vertex should be visited in next iteration.
   */
  marked: boolean;
  /**
   * Cycles that this vertex is involved with.
   */
  cycles?: Set<CycleEdge<Vertex>>;
  /**
   * Successors of this vertex.
   */
  successors?: Map<Entry<Vertex>, number>;
  /**
   * Predecessors of this vertex.
   */
  predecessors?: Set<Entry<Vertex>>;
}

export type SchedulerCb = (fn: (max?: number) => void) => void;

export type VisitorCb<Vertex> = (vertex: Vertex) => void;

/**
 * An incremental DAG that can keep track of circular references and poisoned nodes.
 */
export class Graph<Vertex> {
  /** The data associated with each vertex in the graph. */
  private entries = new Map<Vertex, Entry<Vertex>>();
  /** The first free `order` to use. */
  private nextOrder: number = 0;
  /** Pool of the free node IDs (to be reused). */
  private freeOrderPool: number[] = [];
  /** The DFS stack. */
  private stack: Entry<Vertex>[] = [];
  /** Nodes we visited during the forward traversal. */
  private visitedForward: Entry<Vertex>[] = [];
  /** Nodes we visited during the backward traversal. */
  private visitedBackward: Entry<Vertex>[] = [];
  /** List of all the edges that their insertion introduced a cycle in the graph. */
  private circularEdges = new Set<CycleEdge<Vertex>>();
  private scheduled = false;
  /** Flag that indicates if we're in the middle of `visit()`. */
  private visitInProgress = false;
  /** Marked vertices. */
  private visitQueue = new PriorityQueue<Entry<Vertex>>();

  /**
   * Initialize the graph.
   * @param edges The initial array of edges.
   */
  constructor(
    edges?: [Vertex, Vertex][],
    readonly visitor?: VisitorCb<Vertex>,
    readonly scheduler?: SchedulerCb
  ) {
    if (edges && edges.length) {
      const ordered = toposort(edges);
      for (let i = 0, n = ordered.length; i < n; ++i) this.addNode(ordered[i]);
      for (let i = 0, n = edges.length, e = edges[0]; i < n; e = edges[++i])
        this.addEdge(e[0], e[1]);
    }
    this.visit = this.visit.bind(this);
  }

  /**
   * Return whether `target` can be reached from `source` in this graph,
   * this method only checks vertices whose topological order is between
   * the source and target.
   * @param source The source node.
   * @param target The target node.
   */
  isReachable(source: Vertex, target: Vertex): boolean {
    if (source === target) return this.entries.has(target);
    const targetEntry = this.entries.get(target);
    const sourceEntry = this.entries.get(source);
    // One of the node does not even belong to this graph.
    if (!targetEntry || !sourceEntry) return false;
    if (sourceEntry.order > targetEntry.order) {
      // If the two nodes belong to the same cycle, swap `source` and `target`.
      if (sourceEntry.cycles && targetEntry.cycles) {
        const commonCycles = setIntersect(sourceEntry.cycles, targetEntry.cycles);
        if (commonCycles.length) {
          this.traverseForward(sourceEntry, Infinity);
          const reachable = targetEntry.visited;
          this.cleanAfterForward();
          return reachable;
        }
      }
      return false;
    }
    const reachable = this.traverseForward(sourceEntry, targetEntry.order);
    this.cleanAfterForward();
    return reachable;
  }

  /**
   * Returns `true` if `target` is an immediate forward neighbor of the `source`.
   * @param source The first node.
   * @param target The second node.
   */
  isConnected(source: Vertex, target: Vertex): boolean {
    const sEntry = this.entries.get(source);
    const tEntry = this.entries.get(target);
    return !!sEntry && !!tEntry && !!sEntry.successors && sEntry.successors.has(tEntry);
  }

  /**
   * Returns the immediate successors of the given vertex.
   * @param node The node to lookup.
   */
  getSuccessorsOf(node: Vertex): Vertex[] {
    const entry = this.entries.get(node);
    if (entry && entry.successors) return Array.from(entry.successors.keys()).map((e) => e.vertex);
    return [];
  }

  /**
   * Returns the immediate predecessors of the given vertex.
   * @param node The node to lookup.
   */
  getPredecessorsOf(node: Vertex): Vertex[] {
    const entry = this.entries.get(node);
    if (entry && entry.predecessors) return Array.from(entry.predecessors).map((e) => e.vertex);
    return [];
  }

  /**
   * Add a new node to this graph, returns `false` if the node was already in the graph.
   * @param v The vertex to add.
   * @returns Whether this operation changed the state of this graph.
   */
  addNode(v: Vertex): boolean {
    if (this.entries.has(v)) return false;
    const id = this.freeOrderPool.pop();
    const order = id === undefined ? this.nextOrder++ : id;
    const entry: Entry<Vertex> = {
      vertex: v,
      order,
      visited: false,
      marked: false,
    };
    this.entries.set(v, entry);
    this.markEntry(entry);
    return true;
  }

  /**
   * Delete the given node from this graph, returns `false` if the node did not belong to this
   * graph in the first place.
   * @param v The node to be deleted.
   */
  deleteNode(v: Vertex): boolean {
    const vEntry = this.entries.get(v);
    if (!vEntry) return false;

    // First delete all the forward links to this node.
    if (vEntry.predecessors) {
      for (const e of vEntry.predecessors) deleteMany(e, 'successors', vEntry);
      vEntry.predecessors = undefined;
    }

    if (vEntry.cycles)
      this.updateCycles(vEntry, vEntry.cycles, (e) => e[0] === vEntry || e[1] === vEntry);

    // Now delete the rest of the edges involving this vertex.
    if (vEntry.successors) {
      for (const e of vEntry.successors.keys()) deleteMany(e, 'predecessors', vEntry);
      vEntry.successors = undefined;
    }

    // Delete the data associated with this node and free the `id` so it can be reused later.
    this.entries.delete(v);
    this.freeOrderPool.push(vEntry.order);
    vEntry.marked = false;
    return true;
  }

  /**
   * Insert a new edge in the graph.
   * @param from The source node.
   * @param to The target node.
   */
  addEdge(from: Vertex, to: Vertex) {
    // Don't add the edge if it's a self-loop.
    if (from === to) return false;
    // Ensure that the nodes exits.
    this.addNode(from);
    this.addNode(to);
    const fromEntry = this.entries.get(from)!;
    const toEntry = this.entries.get(to)!;
    // Or if the edge already exists.
    if (fromEntry.successors && fromEntry.successors.has(fromEntry)) {
      counterMapAdd(fromEntry, 'successors', toEntry);
      return true;
    }
    this.addEdgeHelper(fromEntry, toEntry);
    counterMapAdd(fromEntry, 'successors', toEntry);
    setAddMany(toEntry, 'predecessors', fromEntry);
    return true;
  }

  /**
   * Delete an edge from the graph.
   * @param from The source node.
   * @param to The target node.
   */
  deleteEdge(from: Vertex, to: Vertex): boolean {
    const fromEntry = this.entries.get(from);
    const toEntry = this.entries.get(to);
    if (!fromEntry || !toEntry || !fromEntry.successors || !fromEntry.successors.has(toEntry))
      return false;
    counterMapDelete(fromEntry, 'successors', toEntry);
    if (fromEntry.successors && fromEntry.successors.has(toEntry)) return false;
    deleteMany(toEntry, 'predecessors', fromEntry);
    if (fromEntry.cycles && toEntry.cycles) {
      const commonCycles = setIntersect(fromEntry.cycles, toEntry.cycles);
      if (commonCycles.length)
        this.updateCycles(toEntry, commonCycles, (e) => e[0] === fromEntry && e[1] === toEntry);
    }
    return true;
  }

  /**
   * Return the topological order of the nodes in this graph, this method should not be called
   * frequently as this implementation is not optimized for that.
   */
  toposort(): Vertex[] {
    return Array.from(this.entries.values())
      .sort((a, b) => a.order - b.order)
      .map((v) => v.vertex);
  }

  /**
   * Visit all of the pending nodes with the default visitor.
   * @param limit Maximum number of nodes to visit in this iteration.
   */
  visit(limit: number = Infinity) {
    if (limit <= 0 || !Number.isFinite(limit)) limit = Infinity;
    if (!this.visitor) throw new Error('Graph does not have a visitor.');
    if (this.visitInProgress) throw new Error('Graph is already in the middle of another visit.');
    try {
      this.visitInProgress = true;
      let visited = 0;
      while (visited < limit && !this.visitQueue.isEmpty()) {
        const node = this.visitQueue.dequeue()!;
        // If the node has cycles we skip visiting but don't set the node.marked to false,
        // in deleteCyclesFromNode, we enqueue the node if there are no more cycles on it.
        if (node.cycles || !node.marked) continue;
        visited += 1;
        node.marked = false;
        this.visitor(node.vertex);
      }
    } finally {
      this.visitInProgress = false;
      this.scheduled = false;
      // There are still non-circular nodes in the queue so schedule another visit.
      if (!this.visitQueue.isEmpty() && this.scheduler) {
        this.scheduler(this.visit);
        this.scheduled = true;
      }
    }
  }

  /**
   * Mark the given node, so we will visit it in the next visitation process.
   * @param node The node to be revisited.
   */
  mark(node: Vertex): boolean {
    const entry = this.entries.get(node);
    return !!entry && this.markEntry(entry);
  }

  /**
   * Mark all of the successors of the given node.
   * @param node The node to be revisited.
   */
  markSuccessors(node: Vertex, filter?: (v: Vertex) => boolean) {
    const entry = this.entries.get(node);
    if (!entry || !entry.successors) return;
    for (const e of entry.successors.keys()) if (!filter || filter(e.vertex)) this.markEntry(e);
  }

  private markEntry(entry: Entry<Vertex>): boolean {
    if (entry.marked) return false;
    entry.marked = true;
    // The enqueue happens when there are no more cycles on this entry (#deleteCyclesFromNode.)
    if (entry.cycles) return true;
    this.visitQueue.insert(entry.order, entry);
    if (!this.scheduled && this.scheduler) {
      this.scheduler(this.visit);
      this.scheduled = true;
    }
    return true;
  }

  /**
   * Remove/Modify the cycles from the graph and reorder the node to keep them topologically
   * ordered.
   * @param nodeEntry The vertex that is affected, possible links from cycles and this vertex must
   * already be eliminated from the graph.
   * @param base List of the cycles we should operate on.
   * @param removePredicate A callback function that decides whether an entire cycles must be
   * marked as resolved.
   */
  private updateCycles(
    nodeEntry: Entry<Vertex>,
    base: Iterable<CycleEdge<Vertex>>,
    removePredicate: (e: CycleEdge<Vertex>) => boolean
  ) {
    const toBeRemoved: CycleEdge<Vertex>[] = [];
    const cycles: CycleEdge<Vertex>[] = [];

    for (const e of base) {
      if (removePredicate(e)) {
        toBeRemoved.push(e);
        this.circularEdges.delete(e);
        this.stack.push(e[0], e[1]);
      } else {
        cycles.push(e);
      }
    }

    if (toBeRemoved.length) {
      this.traverseForward(null, Infinity);
      for (let i = 0, n = this.visitedForward.length; i < n; ++i) {
        const entry = this.visitedForward[i];
        entry.visited = false;
        this.deleteCyclesFromNode(entry, ...toBeRemoved);
      }
      this.visitedForward = [];
    }

    for (let i = 0, n = cycles.length; i < n; ++i) {
      const edge = cycles[i];
      const [xEntry, yEntry] = edge;
      let doReorder = false;
      this.traverseForward(yEntry, Infinity);

      if (xEntry.visited) {
        // The cycle is not destroyed, we should delete the `edge` only from the elements
        // that are not in the cycle anymore.
        const inCycle = this.visitedForward;
        this.visitedForward = [];
        this.traverseForward(nodeEntry, Infinity);
        for (let i = 0, n = inCycle.length; i < n; ++i) inCycle[i].visited = false;
      } else {
        // The cycle is destroyed.
        doReorder = true;
        this.circularEdges.delete(edge);
        // Make sure we've visited all of the nodes in the cycle.
        if (!nodeEntry.visited) this.stack.push(nodeEntry);
        this.traverseForward(xEntry, Infinity);
      }

      for (let i = 0, n = this.visitedForward.length; i < n; ++i) {
        const entry = this.visitedForward[i];
        entry.visited = false;
        this.deleteCyclesFromNode(entry, edge);
      }

      this.visitedForward = [];

      if (doReorder) this.addEdgeHelper(xEntry, yEntry);
    }
  }

  private addEdgeHelper(xEntry: Entry<Vertex>, yEntry: Entry<Vertex>) {
    if (xEntry.cycles) {
      const delta = setDiff(xEntry.cycles, yEntry.cycles);
      if (delta.length) {
        this.traverseForward(yEntry, Infinity);
        for (let i = 0, n = this.visitedForward.length; i < n; ++i) {
          const entry = this.visitedForward[i];
          this.insertCyclesToNode(entry, ...delta);
        }
      }
    }

    // If we're already sorted, we don't need to make any changes.
    if (xEntry.order <= yEntry.order) {
      if (yEntry.visited) this.cleanAfterForward();
      return;
    }

    if (!yEntry.visited) {
      // Otherwise we've already visited all of the successors of `y`.
      const outOfBoundStack: Entry<Vertex>[] = [];
      this.traverseForward(yEntry, xEntry.order, outOfBoundStack);
    }

    // Discovery
    if (xEntry.visited) {
      // Visit all the remaining elements in the stack.
      this.traverseForward(xEntry, Infinity);

      // If `x` is visited at this point it means there is at least one path in this graph
      // from `y` to `x`, so adding `(x, y)` introduces a cycle.
      const cycle: CycleEdge<Vertex> = [
        xEntry,
        yEntry,
        {
          from: xEntry.vertex,
          to: yEntry.vertex,
        },
      ];
      for (let i = 0, n = this.visitedForward.length; i < n; ++i) {
        const entry = this.visitedForward[i];
        entry.visited = false;
        this.insertCyclesToNode(entry, cycle);
      }
      this.visitedForward = [];
      this.circularEdges.add(cycle);
      return;
    }
    this.traverseBackward(xEntry, yEntry.order);
    this.reorder();
  }

  /**
   * Traverse the graph, starting from the `first` node and moving in the forward direction,
   * returning `true` in case it finds the node with the provided order.
   * This method leaves the structure in a broken state, you should call `cleanAfterForward`
   * to make sure that we're still in a valid state.
   * @param first The source node.
   * @param targetOrder The `order` of the target node.
   * @param outOfBoundStack An optional array that we will use to push elements that we could
   * visit, but ignored because their order was higher than `targetOrder`.
   * @param ub Maximum order of a node that we should visit, only use this parameter when you
   * know what you're doing.
   */
  private traverseForward(
    first: Entry<Vertex> | null,
    targetOrder: number,
    outOfBoundStack?: Entry<Vertex>[],
    ub: number = targetOrder
  ) {
    if (first !== null) this.stack.push(first);
    while (this.stack.length > 0) {
      const n = this.stack.pop()!;
      if (n.visited) continue;
      n.visited = true;
      this.visitedForward.push(n);
      if (!n.successors) continue;
      for (const wEntry of n.successors.keys()) {
        // Reached target.
        if (wEntry.order === targetOrder) return true;
        // Is node not visited?
        if (!wEntry.visited) {
          // ... and in affected region?
          if (wEntry.order <= ub) {
            this.stack.push(wEntry);
          } else if (outOfBoundStack) {
            outOfBoundStack.push(wEntry);
          }
        }
      }
    }
    return false;
  }

  private cleanAfterForward() {
    this.stack = [];
    for (let i = 0, n = this.visitedForward.length; i < n; ++i)
      this.visitedForward[i].visited = false;
    this.visitedForward = [];
  }

  /**
   * Traverse the graph, starting from the `first` node and moving in the backward direction.
   * @param first The first node to visit.
   * @param lb The lowest order to look for.
   */
  private traverseBackward(first: Entry<Vertex>, lb: number) {
    this.stack.push(first);
    while (this.stack.length > 0) {
      const n = this.stack.pop()!;
      if (n.visited) continue;
      n.visited = true;
      this.visitedBackward.push(n);
      if (!n.predecessors) continue;
      for (const wEntry of n.predecessors) {
        // is w unvisited and in affected region?
        if (!wEntry.visited && lb < wEntry.order) this.stack.push(wEntry);
      }
    }
  }

  /**
   * Called by `addEdgeHelper` to reorder the vertices so they are still
   * topologically sorted.
   */
  private reorder() {
    // Sort sets to preserve original order of elements.
    this.visitedBackward.sort((v1, v2) => v1.order - v2.order);
    this.visitedForward.sort((v1, v2) => v1.order - v2.order);

    const L: Entry<Vertex>[] = this.visitedBackward.concat(this.visitedForward);
    const R: number[] = merge(this.visitedBackward, this.visitedForward);
    const Q: Entry<Vertex>[] = [];

    // Allocate vertices in L starting from lowest
    for (let i = 0, n = L.length; i < n; ++i) {
      const w = L[i];
      w.order = R[i];
    }

    for (let i = 0, n = Q.length; i < n; ++i) {
      const w = Q[i];
      this.visitQueue.insert(w.order, w);
    }

    // Clean the arrays.
    this.visitedBackward.length = 0;
    this.visitedForward.length = 0;
  }

  private insertCyclesToNode(entry: Entry<Vertex>, ...cycles: CycleEdge<Vertex>[]) {
    const set = entry.cycles;
    if (set) {
      for (const cycle of cycles) set.add(cycle);
    } else {
      entry.cycles = new Set(cycles);
    }
  }

  private deleteCyclesFromNode(entry: Entry<Vertex>, ...cycles: CycleEdge<Vertex>[]) {
    const set = entry.cycles;
    if (!set) return;
    for (const cycle of cycles) set.delete(cycle);
    if (set.size > 0) return;
    entry.cycles = undefined;
    if (entry.marked) {
      entry.marked = false;
      this.markEntry(entry);
    }
  }
}

/**
 * Performs a merge sort step of two arrays, with the actual array value stored
 * in the key property of the array item.
 */
function merge<V>(arr1: Entry<V>[], arr2: Entry<V>[]): number[] {
  const res: number[] = [];
  const len1 = arr1.length;
  const len2 = arr2.length;
  let i1 = 0;
  let i2 = 0;
  // Push the smaller value from both arrays to the result
  // as long as there remains at least one element in one
  // array.
  while (i1 < len1 && i2 < len2) {
    const o1 = arr1[i1].order;
    const o2 = arr2[i2].order;
    if (o1 < o2) {
      i1 += 1;
      res.push(o1);
    } else {
      i2 += 1;
      res.push(o2);
    }
  }
  // Push the remaining elements, if any, to the result.
  while (i1 < len1) {
    res.push(arr1[i1].order);
    i1 += 1;
  }
  while (i2 < len2) {
    res.push(arr2[i2].order);
    i2 += 1;
  }
  // Return sorted array.
  return res;
}

function setDiff<T>(s1: ReadonlySet<T> | undefined, s2: ReadonlySet<T> | undefined): T[] {
  if (!s1) return [];
  if (!s2 || s2.size === 0) return Array.from(s1);
  const result: T[] = [];
  for (const item of s1) if (!s2.has(item)) result.push(item);
  return result;
}

function setIntersect<T>(s1: ReadonlySet<T>, s2: ReadonlySet<T>): T[] {
  const result: T[] = [];
  for (const item of s1) if (s2.has(item)) result.push(item);
  return result;
}

type SetMember<T> = T extends Set<infer V> ? V : never;
type SetMapMember<T> = T extends Set<infer V> ? V : T extends Map<infer K, any> ? K : never;
type CounterMapKey<T> = T extends Map<infer K, number> ? K : never;

/**
 * Add one or more elements to a set.
 * @param ref The object that owns the set.
 * @param key The field in the `ref` that is the set subject to our changes.
 * @param items Items to be inserted in the set.
 */
function setAddMany<O, K extends keyof O>(ref: O, key: K, ...items: SetMember<O[K]>[]) {
  const set: any = ref[key];
  if (set) {
    for (const item of items) set.add(item);
  } else {
    ref[key] = new Set(items) as any;
  }
}

/**
 * Delete one or more keys from a map or a set and delete the entire map from the object
 * if there is no more elements in the collection.
 * @param ref The object that owns the map/set.
 * @param key The field in the `ref` that is the set or map which is subject to change.
 * @param items List of keys to be deleted.
 */
function deleteMany<O, K extends keyof O>(ref: O, key: K, ...items: SetMapMember<O[K]>[]) {
  const set: Set<SetMapMember<O[K]>> | undefined = ref[key] as any;
  if (set) {
    for (const item of items) set.delete(item);
    if (set.size === 0) ref[key] = undefined as any;
  }
}

/**
 * Add the given element to a counter map by increasing its value in the map.
 * @param ref The object that owns the map.
 * @param key Name of the field in the `ref` that is the map subject to our changes.
 * @param item Item to be inserted.
 */
function counterMapAdd<O, K extends keyof O>(ref: O, key: K, item: CounterMapKey<O[K]>) {
  let map: Map<CounterMapKey<O[K]>, number> | undefined = ref[key] as any;
  if (map) {
    const current = map.get(item) || 0;
    map.set(item, current + 1);
  } else {
    map = new Map();
    map.set(item, 1);
    ref[key] = map as any;
  }
}

/**
 * Remove an item from a counter by decreasing its value and removing it when the value
 * reaches 0.
 * @param ref The object that contains the counter.
 * @param key The field in the object that is the counter.
 * @param item Item to be deleted.
 */
function counterMapDelete<O, K extends keyof O>(ref: O, key: K, item: CounterMapKey<O[K]>) {
  const map: Map<CounterMapKey<O[K]>, number> | undefined = ref[key] as any;
  if (map) {
    const current = map.get(item) || 0;
    if (current <= 1) {
      map.delete(item);
    } else {
      map.set(item, current - 1);
    }
    if (map.size === 0) ref[key] = undefined as any;
  }
}

/**
 * Return the topological ordering of nodes of a graph, this function ignores
 * circular references.
 * @param edges List of edges to form the graph.
 */
function toposort<Node>(edges: [Node, Node][]): Node[] {
  const entries = new Map<Node, { inDegree: number; successors: Set<Node> }>();
  const queue: Node[] = [];
  const result: Node[] = [];

  for (let i = 0, n = edges.length, e = edges[0]; i < n; e = edges[++i]) {
    if (!entries.has(e[0])) {
      entries.set(e[0], {
        inDegree: 0,
        successors: new Set(),
      });
    }
    if (!entries.has(e[1])) {
      entries.set(e[1], {
        inDegree: 0,
        successors: new Set(),
      });
    }
    entries.get(e[0])!.successors.add(e[1]);
  }

  for (const entry of entries.values())
    for (const v of entry.successors) entries.get(v)!.inDegree += 1;

  for (const [node, entry] of entries) if (entry.inDegree === 0) queue.push(node);

  for (let i = 0; i < queue.length; ++i) {
    const u = queue[i];
    result.push(u);
    for (const v of entries.get(u)!.successors) {
      const ev = entries.get(v)!;
      if (--ev.inDegree === 0) queue.push(v);
    }
  }

  return result;
}