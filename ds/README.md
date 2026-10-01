# IndexedPriorityQueue

`com.salesforce.einstein.ds.IndexedPriorityQueue<T>` is a binary **min-heap** that also keeps an
index from each element to its slot(s) in the heap array. That index turns the operations a plain
`java.util.PriorityQueue` does in O(N) — `remove(Object)`, `contains(Object)`, re-prioritising an
element — into O(log N) / O(1).

| Operation | `java.util.PriorityQueue` | `IndexedPriorityQueue` |
|---|---|---|
| `offer` / `poll` | O(log N) | O(log N) |
| `peek` | O(1) | O(1) |
| `contains(Object)` | O(N) | **O(1)** |
| `remove(Object)` | O(N) | **O(log N)** |
| re-key an element | remove + re-add, O(N) | **`update(e)`, O(log N)** |

Costs are expected-time (they rely on `HashMap`). With *k* equal copies of an element, `update` is
O(k log N + k²); everything else stays O(log N) regardless of duplicates.

## Design

```mermaid
classDiagram
    class AbstractQueue~T~ {
        <<java.util>>
        +add(T) boolean
        +addAll(Collection) boolean
        +element() T
        +remove() T
    }

    class IndexedPriorityQueue~T~ {
        -ArrayList~T~ heap
        -Map&lt;T, Set&lt;Integer&gt;&gt; positionMap
        -Comparator~T~ comparator
        -int modCount
        +offer(T) boolean
        +poll() T
        +peek() T
        +contains(Object) boolean
        +remove(Object) boolean
        +update(T) boolean
        +size() int
        +clear() void
        +iterator() Iterator~T~
        -removeAt(int) T
        -siftUp(int) int
        -siftDown(int) int
        -move(int, int) void
        -positionsOf(Object) Set~Integer~
        -indexOfInstance(T) int
    }

    class Itr {
        -int cursor
        -int lastRet
        -int expectedModCount
        -ArrayDeque~T~ forgetMeNot
        -T lastRetElt
        +hasNext() boolean
        +next() T
        +remove() void
    }

    AbstractQueue <|-- IndexedPriorityQueue
    IndexedPriorityQueue *-- Itr : inner class
    IndexedPriorityQueue o-- "0..1" Comparator : orders by
```

The queue keeps two structures that must always agree:

| Field | Type | Role |
|---|---|---|
| `heap` | `ArrayList<T>` | The implicit binary tree. `heap[0]` is the minimum. |
| `positionMap` | `HashMap<T, LinkedHashSet<Integer>>` | element → every slot in `heap` holding an equal element. A key is present **iff** its set is non-empty. |
| `comparator` | `Comparator<? super T>` or `null` | Ordering; `null` means natural ordering (elements must be `Comparable`, checked on `offer`). |
| `modCount` | `int` | Bumped on every structural change so iterators can fail fast. |

**Invariant:** for every `i`, `positionMap.get(heap.get(i))` contains `i`, and nothing else is in
the map. Every method that touches `heap` updates `positionMap` in the same step.

### Why `LinkedHashSet<Integer>` per key?

Duplicates are allowed, and equal elements share one set. The set needs O(1) `add` / `remove`
(done at every sift step) and O(1) "give me any slot" for `remove(Object)`. A `TreeSet` costs
O(log k) per operation. A plain `HashSet` makes `iterator().next()` scan buckets, which gets slow
once the set has shrunk from a large size. `LinkedHashSet` is O(1) for all three.

### Key-stability contract

`equals` / `hashCode` of an element must **not** change while it is in the queue, or the
`positionMap` lookup breaks. Its *priority* (what the comparator reads) may change, as long as
`update(element)` is called afterwards.

## How the heap maps onto the array

A complete binary tree is stored level by level in `heap`, with no pointers:

| Relation | Index |
|---|---|
| parent of `i` | `(i - 1) >>> 1` |
| left child of `i` | `2i + 1` |
| right child of `i` | `2i + 2` |
| leaves | `i >= size / 2` (so `siftDown` loops only while `i < size >>> 1`) |

Example after `offer(5)`, `offer(3)`, `offer(8)`, `offer(1)`, `offer(4)`:

```mermaid
flowchart TB
    subgraph tree["Logical tree (node shows value and array slot)"]
        direction TB
        n0(("1<br/>[0]")) --> n1(("3<br/>[1]"))
        n0 --> n2(("8<br/>[2]"))
        n1 --> n3(("5<br/>[3]"))
        n1 --> n4(("4<br/>[4]"))
    end

    subgraph array["heap (ArrayList)"]
        direction LR
        a0["[0] = 1"] --- a1["[1] = 3"] --- a2["[2] = 8"] --- a3["[3] = 5"] --- a4["[4] = 4"]
    end

    subgraph index["positionMap (HashMap)"]
        direction LR
        k1["1 → {0}"] ~~~ k3["3 → {1}"] ~~~ k8["8 → {2}"] ~~~ k5["5 → {3}"] ~~~ k4["4 → {4}"]
    end

    tree ~~~ array ~~~ index
```

## Sifting: moving a hole instead of swapping

`siftUp` / `siftDown` don't swap pairs (that would be 4 index updates per level). Instead:

1. Detach the sifted element's slot from its position set.
2. Walk up/down, shifting each displaced neighbour one level with `move(from, to)`, which writes
   `heap[to]` and moves one index in that neighbour's set (2 updates per level).
3. Write the element into the final slot and add that slot to its set once.
4. Return the final index. `removeAt` uses it to tell whether the element moved, and which way.

The position set is left in `positionMap` while it is briefly empty, which avoids removing and
re-creating it on every sift. This also works when the sifted element and a neighbour are equal
and share one set.

```mermaid
flowchart TD
    S["siftDown(i)"] --> D["e = heap[i]<br/>slots(e).remove(i)"]
    D --> L{"i < size/2 ?<br/>(has a child)"}
    L -- no --> P
    L -- yes --> C["c = smaller child of i"]
    C --> Q{"e <= heap[c] ?"}
    Q -- yes --> P["heap[i] = e<br/>slots(e).add(i)<br/>return i"]
    Q -- no --> M["move(c, i):<br/>heap[i] = heap[c]<br/>slots(heap[c]): c → i"]
    M --> N["i = c"] --> L
```

## Sequence diagrams

### `offer(e)`

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Q as IndexedPriorityQueue
    participant H as heap (ArrayList)
    participant M as positionMap

    Client->>Q: offer(e)
    Q->>Q: reject null / non-Comparable
    Q->>Q: modCount++
    Q->>H: add(e) at i = size-1
    Q->>M: computeIfAbsent(e).add(i)
    Q->>Q: siftUp(i)
    loop while i > 0 and e < heap[parent]
        Q->>H: heap[i] = heap[parent]
        Q->>M: slots(heap[parent]): parent → i
        Note over Q: i = parent
    end
    Q->>H: heap[i] = e
    Q->>M: slots(e).add(i)
    Q-->>Client: true
```

### `poll()` on the example array `[1, 3, 8, 5, 4]`

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Q as IndexedPriorityQueue
    participant H as heap (ArrayList)
    participant M as positionMap

    Client->>Q: poll()
    Q->>H: root = heap[0] (1)
    Q->>Q: removeAt(0)
    Q->>M: remove slot 0 from 1 (set empty, key dropped)
    Q->>H: moved = remove(last) (4), heap = [1, 3, 8, 5]
    Q->>M: remove slot 4 from 4
    Q->>H: heap[0] = 4, heap = [4, 3, 8, 5]
    Q->>M: slots(4).add(0)
    Q->>Q: siftDown(0)
    Note over Q,H: children 3 and 8, smaller is 3 at [1], 4 > 3
    Q->>H: heap[0] = 3
    Q->>M: slots(3): 1 → 0
    Note over Q,H: i = 1, only child is 5 at [3], 4 <= 5, stop
    Q->>H: heap[1] = 4, heap = [3, 4, 8, 5]
    Q->>M: slots(4).add(1)
    Q-->>Q: final index 1 != 0, no siftUp needed
    Q-->>Client: 1
```

Resulting state:

| | [0] | [1] | [2] | [3] |
|---|---|---|---|---|
| `heap` | 3 | 4 | 8 | 5 |

`positionMap = {3 → {0}, 4 → {1}, 8 → {2}, 5 → {3}}`

### `remove(o)` and `update(e)`

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Q as IndexedPriorityQueue
    participant M as positionMap
    participant H as heap (ArrayList)

    Client->>Q: remove(o)
    Q->>M: positionsOf(o)
    alt not present
        M-->>Q: null
        Q-->>Client: false
    else present
        M-->>Q: slots
        Q->>Q: removeAt(slots.iterator().next())
        Note over Q,H: fill hole with tail, siftDown, then siftUp if it didn't move
        Q-->>Client: true
    end

    Note over Client: caller changes e's priority in place
    Client->>Q: update(e)
    Q->>M: get(e)
    M-->>Q: slots
    Q->>H: snapshot instances at each slot
    loop each instance
        Q->>Q: i = indexOfInstance(instance) (by identity)
        Q->>Q: if siftDown(i) == i then siftUp(i)
    end
    Q-->>Client: true
```

`update` snapshots the instances first because equal-but-distinct objects share one set, and
sifting one of them reshuffles the slots of the others.

## `removeAt(index)`: the core removal

```mermaid
flowchart TD
    A["removeAt(index)"] --> B["modCount++<br/>drop index from slots(heap[index])"]
    B --> C["moved = heap.remove(last)"]
    C --> D{"index == last ?"}
    D -- yes --> R0["return null"]
    D -- no --> E["slots(moved): last → index<br/>heap[index] = moved"]
    E --> F["j = siftDown(index)"]
    F --> G{"j == index ?"}
    G -- no --> R0
    G -- yes --> H["j = siftUp(index)"]
    H --> I{"j < index ?"}
    I -- yes --> R1["return moved<br/>(it moved above index)"]
    I -- no --> R0
```

The return value only matters to the iterator; `poll` and `remove(Object)` ignore it.

## Iterator

The iterator walks `heap` in array order, which is **not** sorted order. `Iterator.remove()` uses
the same technique as `java.util.PriorityQueue`:

- If the tail element filling the hole stays at or below the cursor, the cursor steps back one so
  that slot is visited again.
- If the tail element moves *above* the cursor (`removeAt` returned it), it would be skipped, so it
  goes into `forgetMeNot` and is returned after the array is used up.
- Any change made outside the iterator bumps `modCount`, and the iterator then throws
  `ConcurrentModificationException`.

```mermaid
stateDiagram-v2
    [*] --> ArrayPhase
    ArrayPhase --> ArrayPhase: next() returns heap[cursor++]
    ArrayPhase --> ArrayPhase: remove() and tail stayed low, cursor--
    ArrayPhase --> ArrayPhase: remove() and tail moved up, push to forgetMeNot
    ArrayPhase --> ReplayPhase: cursor == size and forgetMeNot not empty
    ArrayPhase --> [*]: cursor == size and forgetMeNot empty
    ReplayPhase --> ReplayPhase: next() polls forgetMeNot
    ReplayPhase --> ReplayPhase: remove() calls removeAt(indexOfInstance(e))
    ReplayPhase --> [*]: forgetMeNot empty
```

## Usage

```java
// Natural ordering
IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
q.addAll(List.of(5, 3, 8, 1, 4));
q.remove(8);        // O(log N), not O(N)
q.contains(3);      // O(1)
q.poll();           // 1

// Mutable priority: equality must not depend on the priority field
IndexedPriorityQueue<Task> tasks = new IndexedPriorityQueue<>(Comparator.comparingInt(t -> t.priority));
tasks.add(task);
task.priority = 0;  // change in place...
tasks.update(task); // ...then restore heap order in O(log N)
```

Not thread-safe. `null` elements are rejected.

## Tests

`src/test/java/com/salesforce/einstein/ds/IndexedPriorityQueueTest.java` (JUnit 5) covers
ordering, comparators, duplicates, `update`, iterator removal and fail-fast behaviour, plus
randomised runs checked against `java.util.PriorityQueue`.

```bash
JAVA_HOME=<path-to-jdk-17> mvn test
```
