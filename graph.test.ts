import test, { ExecutionContext } from 'ava';

import { Graph } from '../graph';

function testTopologicalOrdering<V>(t: ExecutionContext, graph: Graph<V>) {
  const order = graph.toposort();
  const visited = new Set<V>();
  for (const node of order) {
    visited.add(node);
    for (const predecessor of graph.getPredecessorsOf(node))
      t.is(visited.has(predecessor), true, 'All predecessors must come before the node.');
  }
}

test('Full', (t) => {
  const graph = new Graph<string>();

  // A -> B
  graph.addEdge('A', 'B');
  t.is(graph.isConnected('A', 'B'), true);
  t.is(graph.isConnected('B', 'A'), false);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), false);
  t.is(graph.isConnected('B', 'C'), false);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), false);
  t.is(graph.isReachable('A', 'C'), false);
  t.is(graph.isReachable('C', 'A'), false);
  t.is(graph.isReachable('B', 'C'), false);
  t.is(graph.isReachable('C', 'B'), false);
  testTopologicalOrdering(t, graph);

  // A -> B -> C
  graph.addEdge('B', 'C');
  t.is(graph.isConnected('A', 'B'), true);
  t.is(graph.isConnected('B', 'A'), false);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), false);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), false);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), false);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), false);
  testTopologicalOrdering(t, graph);

  // A -> B -> C
  // ^________/
  graph.addEdge('C', 'A');
  t.is(graph.isConnected('A', 'B'), true);
  t.is(graph.isConnected('B', 'A'), false);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [['A', 'B', 'C']]);

  // +----+
  // V    |
  // A -> B -> C
  // ^_________+
  graph.addEdge('B', 'A');
  t.is(graph.isConnected('A', 'B'), true);
  t.is(graph.isConnected('B', 'A'), true);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [
  //     ['A', 'B', 'C'],
  //     ['A', 'B'],
  //   ]);

  // A -> B -> C
  // ^_________+
  graph.deleteEdge('B', 'A');
  t.is(graph.isConnected('A', 'B'), true);
  t.is(graph.isConnected('B', 'A'), false);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [['A', 'B', 'C']]);

  // B -> C -> A
  // \_________^
  graph.deleteEdge('A', 'B');
  graph.addEdge('B', 'A');
  t.is(graph.isConnected('A', 'B'), false);
  t.is(graph.isConnected('B', 'A'), true);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), false);
  t.is(graph.isReachable('A', 'B'), false);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), false);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), false);
  testTopologicalOrdering(t, graph);

  // +----+
  // V    |
  // B -> C -> A
  // \_________^
  graph.addEdge('C', 'B');
  t.is(graph.isConnected('A', 'B'), false);
  t.is(graph.isConnected('B', 'A'), true);
  t.is(graph.isConnected('A', 'C'), false);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'B'), false);
  t.is(graph.isReachable('A', 'C'), false);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [['B', 'C']]);

  // +----+ +---+
  // V    | V   |
  // B ->  C -> A
  // \__________^
  graph.addEdge('A', 'C');
  t.is(graph.isConnected('A', 'B'), false);
  t.is(graph.isConnected('B', 'A'), true);
  t.is(graph.isConnected('A', 'C'), true);
  t.is(graph.isConnected('C', 'A'), true);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), true);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [
  //     ['B', 'C'],
  //     ['A', 'C'],
  //     ['A', 'C', 'B'],
  //   ]);

  // +----+
  // V    |
  // B -> C <- A
  // \_________^
  graph.deleteEdge('C', 'A');
  t.is(graph.isConnected('A', 'B'), false);
  t.is(graph.isConnected('B', 'A'), true);
  t.is(graph.isConnected('A', 'C'), true);
  t.is(graph.isConnected('C', 'A'), false);
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), true);
  t.is(graph.isReachable('A', 'B'), true);
  t.is(graph.isReachable('B', 'A'), true);
  t.is(graph.isReachable('A', 'C'), true);
  t.is(graph.isReachable('C', 'A'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [
  //     ['B', 'C'],
  //     ['A', 'C', 'B'],
  //   ]);

  // B <-> C
  graph.deleteNode('A');
  t.is(graph.isConnected('B', 'C'), true);
  t.is(graph.isConnected('C', 'B'), true);
  t.is(graph.isReachable('B', 'C'), true);
  t.is(graph.isReachable('C', 'B'), true);
  //   t.deepEqual(graph.cycles.toJS(), [['B', 'C']]);
  t.deepEqual(Array.from(graph.getSuccessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getSuccessorsOf('B')), ['C']);
  t.deepEqual(Array.from(graph.getPredecessorsOf('B')), ['C']);
  t.deepEqual(Array.from(graph.getSuccessorsOf('C')), ['B']);
  t.deepEqual(Array.from(graph.getPredecessorsOf('C')), ['B']);
});

test('Self loop', (t) => {
  const graph = new Graph<string>();
  graph.addNode('A');
  t.is(graph.addEdge('A', 'A'), false);
  t.is(graph.isConnected('A', 'A'), false);
  t.is(graph.isReachable('A', 'A'), true);
  t.is(graph.deleteEdge('A', 'A'), false);
  t.is(graph.isConnected('A', 'A'), false);
  t.is(graph.isReachable('A', 'A'), true);
  testTopologicalOrdering(t, graph);
});

test('deleteNode', (t) => {
  // A -> B -> C
  let graph = new Graph();
  graph.addEdge('A', 'B');
  graph.addEdge('B', 'C');
  t.deepEqual(Array.from(graph.getSuccessorsOf('A')), ['B']);
  t.deepEqual(Array.from(graph.getPredecessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getSuccessorsOf('B')), ['C']);
  t.deepEqual(Array.from(graph.getPredecessorsOf('B')), ['A']);
  t.deepEqual(Array.from(graph.getSuccessorsOf('C')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('C')), ['B']);
  // A C
  graph.deleteNode('B');
  t.deepEqual(Array.from(graph.getSuccessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getSuccessorsOf('B')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('B')), []);
  t.deepEqual(Array.from(graph.getSuccessorsOf('C')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('C')), []);
  // A -> B -> C
  graph = new Graph();
  graph.addEdge('A', 'B');
  graph.addEdge('B', 'C');
  // A -> B
  graph.deleteNode('C');
  t.deepEqual(Array.from(graph.getSuccessorsOf('A')), ['B']);
  t.deepEqual(Array.from(graph.getPredecessorsOf('A')), []);
  t.deepEqual(Array.from(graph.getSuccessorsOf('B')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('B')), ['A']);
  t.deepEqual(Array.from(graph.getSuccessorsOf('C')), []);
  t.deepEqual(Array.from(graph.getPredecessorsOf('C')), []);
});
