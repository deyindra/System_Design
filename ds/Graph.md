# Graph: a generic graph with cycle-safe BFS/DFS iterators

Package `com.salesforce.einstein.ds.graph`, in the `ds` module: [`Graph.java`](src/main/java/com/salesforce/einstein/ds/graph/Graph.java)
and its implementation and iterators, pure JDK. Tested by
[`GraphTest`](src/test/java/com/salesforce/einstein/ds/graph/GraphTest.java) (21 tests) and
[`TraversalTest`](src/test/java/com/salesforce/einstein/ds/graph/TraversalTest.java) (11).

**Used by** [graphexecutor](../graphexecutor/README.md), whose executors all run over a `Graph`, and the
[webcrawler](../webcrawler/README.md), whose sitemap is one.

| Section | Main types |
|---|---|
| [1. Graph](#1-graph) | `Graph`, `AdjacencyGraph` |
| [2. BFS / DFS](#2-bfs--dfs-iterators-cycle-safe) | `TraversalIterator`, `BfsIterator`, `DfsIterator` |

## 1. Graph

Direction and weighting are two independent **properties of a graph instance**, not separate classes:

```java
Graph<String> g = Graph.directed();            // unweighted, unidirectional
Graph<String> g = Graph.undirected();          // unweighted, bidirectional
Graph<String> g = Graph.weightedDirected();    // weighted,   unidirectional
Graph<String> g = Graph.weightedUndirected();  // weighted,   bidirectional
g.isDirected(); g.isWeighted();
```

`Graph<T>` is the interface; `AdjacencyGraph<T>` (package-private) is the single implementation
behind all four factories. One class configured by two flags avoids a class per combination
(2 × 2 = 4 today, 8 with a third property).

| Operation | Semantics | Cost |
|---|---|---|
| `addNode(n)` | false if present | O(1) |
| `removeNode(n)` | also removes every incident edge | O(degree) |
| `updateNode(old, new)` | renames, moving every edge and weight; throws if `new` exists | O(degree) |
| `addEdge(a, b)` | **unweighted only**; adds missing endpoints; false if edge exists | O(1) |
| `addEdge(a, b, w)` | **weighted only**; false (weight kept) if edge exists | O(1) |
| `updateEdge(a, b, w)` | **weighted only**; false if no such edge | O(1) |
| `removeEdge(a, b)` | false if no such edge | O(1) |
| `edgeWeight(a, b)` | `Optional<Double>`: empty if unweighted or no such edge | O(1) |
| `successors` / `predecessors`, `outDegree` / `inDegree` | undirected: both are the neighbors / degree | O(1) |
| `subgraph(nodes)` | independent copy with the same properties and weights | O(V + E) |

Calling the other kind's edge method throws `UnsupportedOperationException`. So a weighted graph
*must* have a weight on every edge, and an unweighted graph never has one.

**Storage:** `Map<T, Map<T, Optional<Double>>>`. Weighted edges hold `Optional.of(w)`; unweighted
edges hold `Optional.empty()`, a shared singleton, so unweighted graphs pay nothing for it.

**The direction trick.** Each edge `a -> b` is written as `out[a][b]` and `in[b][a]`. Directed: `in`
is a separate reverse index (that's what makes `removeNode` O(degree)). Undirected: `in` *is* `out`,
so writing `in[b][a]` is exactly the mirror entry. Every method is written once and is correct for both.

`LinkedHashMap` everywhere, so traversal order is deterministic (insertion order).

## 2. BFS / DFS iterators (cycle-safe)

`TraversalIterator<T>` (template) → `BfsIterator<T>`, `DfsIterator<T>`.

- **Cycles:** a `visited` set; each node is returned once. O(V + E) time, O(V) space.
- **BFS** marks visited on *enqueue*, so a node never sits in the queue twice.
- **DFS** is iterative (no stack overflow on a 200k-node chain) and keeps a stack of *neighbor
  iterators*, giving the exact recursive preorder with stack depth = path length. The "push all
  neighbors" shortcut gets the sibling order backwards and can push one node many times.
- **Lazy** (`hasNext` computes one step), **fail-fast** (CME if the graph changes), and multi-root:
  `bfs()`/`dfs()` start a new tree at each unreached node, so they cover disconnected graphs.
- They take a `node -> neighbors` function, so they also work on *views* of a graph. The executor
  ([graphexecutor](../graphexecutor/README.md)) uses this to walk a directed graph's edges in both directions.
