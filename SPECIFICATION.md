# Local Communication Channels — Specification

## 1. Purpose

This project specifies a communication layer allowing concurrent tasks to exchange sequences of bytes through bidirectional communication channels.

The communication API is based on three abstractions:

* `Broker`: creates connections between named communication endpoints.
* `Channel`: transfers bytes between two connected tasks.
* `Task`: executes a `Runnable` in a thread associated with a broker.

The first implementation is local: all brokers, tasks and channels execute in the same Java Virtual Machine. However, applications must use only the public abstractions so that another implementation could later use operating-system processes or network sockets.

This document defines the externally observable behavior of these abstractions. It describes what the classes must do, independently of how they are implemented.

---

## 2. Public API

The project provides the following abstract API:

```java
abstract class Broker {
    Broker(String name);

    abstract Channel accept(int port);

    abstract Channel connect(String name, int port);
}
```

```java
abstract class Channel {
    abstract int read(byte[] bytes, int offset, int length);

    abstract int write(byte[] bytes, int offset, int length);

    abstract void disconnect();

    abstract boolean disconnected();
}
```

```java
abstract class Task extends Thread {
    Task(Broker broker, Runnable runnable);

    static Broker getBroker();
}
```

The concrete local implementation must respect all requirements defined in this document.

---

## 3. Terminology

### 3.1 Task

A task is an independently executing Java thread associated with exactly one broker.

### 3.2 Broker

A broker is a named communication endpoint. It allows tasks to accept incoming connections and connect to other brokers.

### 3.3 Port

A port identifies a communication service inside a broker. A broker name identifies the destination broker, while a port identifies a particular connection point within that broker.

The pair:

```text
(broker name, port)
```

uniquely identifies a local connection destination.

### 3.4 Channel

A channel is one endpoint of a bidirectional byte stream connecting two tasks.

Each connection produces two channel endpoints:

```text
Endpoint A <----------------------> Endpoint B
```

Data written through endpoint A can be read through endpoint B, and data written through endpoint B can be read through endpoint A.

### 3.5 Local endpoint

The local endpoint is the `Channel` object on which a method is called.

### 3.6 Remote endpoint or peer

The remote endpoint is the other `Channel` object belonging to the same connection.

---

## 4. General communication guarantees

### 4.1 Bidirectional communication

A connection supports communication in both directions.

Writing in one direction does not prevent communication in the opposite direction.

For example, the following operations may occur independently:

```text
Task A writes to Task B
Task B writes to Task A
```

### 4.2 FIFO ordering

Each direction of a channel follows a first-in, first-out policy.

If endpoint A successfully writes the byte sequences:

```text
[1, 2, 3]
```

and then:

```text
[4, 5]
```

endpoint B must observe:

```text
[1, 2, 3, 4, 5]
```

The bytes must not be reordered.

### 4.3 Lossless communication

Every byte reported as successfully written must remain available to the peer until one of the following occurs:

* The peer reads the byte.
* The peer explicitly disconnects and abandons the connection.
* The entire local execution environment terminates.

The implementation must not silently discard successfully written bytes during normal communication.

### 4.4 Stream semantics

A channel transports a stream of bytes. It does not preserve application-level message boundaries.

For example:

```java
channel.write(first, 0, first.length);
channel.write(second, 0, second.length);
```

The peer may receive the resulting bytes using one `read` call or several `read` calls.

Applications that require messages must define their own framing protocol, such as:

* Fixed-size messages
* A message-length prefix
* A delimiter
* A serialization format

### 4.5 Locality

In this version, all brokers and tasks exist in the same Java Virtual Machine.

The specification does not require communication across separate processes or machines.

### 4.6 Thread visibility

When a method returns successfully, its effects must eventually be visible to the peer.

Shared communication state must not depend on stale thread-local processor caches.

---

## 5. Argument validation

The `read` and `write` methods receive:

```java
byte[] bytes
int offset
int length
```

The range is valid when all of the following conditions hold:

```text
bytes is not null
offset >= 0
length >= 0
offset <= bytes.length - length
```

This permits:

```text
offset == bytes.length
length == 0
```

### 5.1 Null array

If `bytes` is `null`, `read` and `write` must throw:

```java
NullPointerException
```

### 5.2 Invalid range

If `offset` or `length` describes a range outside the array, the method must throw:

```java
IndexOutOfBoundsException
```

Invalid cases include:

```text
offset < 0
length < 0
offset > bytes.length
length > bytes.length - offset
```

### 5.3 Validation order

Argument validation occurs before the method attempts to transfer data or wait for channel state changes.

Therefore, invalid arguments must be rejected even if the channel is disconnected.

---

## 6. Broker specification

## 6.1 Broker identity

Every broker has a non-null, non-empty name.

Broker names are case-sensitive. Therefore:

```text
"Server"
```

and:

```text
"server"
```

represent different broker names.

A name containing only whitespace is considered invalid.

### 6.1.1 Unique names

Two active brokers must not have the same name within the local execution environment.

Attempting to create a broker with an already registered name must fail with:

```java
IllegalArgumentException
```

### 6.1.2 Invalid name

Creating a broker with a `null`, empty or whitespace-only name must fail with:

```java
IllegalArgumentException
```

---

## 6.2 Port validity

A valid port is an integer in the inclusive range:

```text
0 to 65535
```

Calling `accept` or `connect` with an invalid port must fail with:

```java
IllegalArgumentException
```

---

## 6.3 `Channel accept(int port)`

### 6.3.1 Purpose

`accept` waits for and accepts an incoming connection on the specified local port.

### 6.3.2 Preconditions

The port must be valid.

Only one invocation of `accept` may wait on the same broker and port at a given time.

### 6.3.3 Blocking behavior

If no connection request is available, `accept` blocks until a compatible `connect` operation targets the same broker and port.

The following execution order must work:

```text
accept first, connect later
```

The following order must also work:

```text
connect first, accept later
```

### 6.3.4 Successful result

When a connection is established, `accept` returns the server-side `Channel` endpoint.

The corresponding call to `connect` returns the peer endpoint of the same connection.

### 6.3.5 Connection ordering

If several clients connect to the same broker and port before they are accepted, connections must be returned by `accept` in FIFO order.

The first pending connection request must be accepted first.

### 6.3.6 Concurrent accept on the same port

If another thread is already executing `accept` on the same broker and port, the second invocation must fail with:

```java
IllegalStateException
```

Concurrent `accept` operations on different ports are allowed.

### 6.3.7 Interruption

If the thread is interrupted while waiting:

1. The method must restore the thread’s interrupted status.
2. The pending accept operation must be cancelled.
3. The method must throw `IllegalStateException` with the original `InterruptedException` as its cause.

Example:

```java
Thread.currentThread().interrupt();

throw new IllegalStateException(
    "Interrupted while waiting for a connection",
    exception
);
```

---

## 6.4 `Channel connect(String name, int port)`

### 6.4.1 Purpose

`connect` requests a connection to the broker identified by `name` on the specified port.

### 6.4.2 Preconditions

The destination name must be non-null and non-empty.

The port must be valid.

### 6.4.3 Unknown destination

If no broker with the requested name exists when `connect` is called, the method must throw:

```java
IllegalArgumentException
```

The method does not wait for a broker with that name to be created later.

### 6.4.4 Blocking behavior

If the destination broker exists but has not yet called `accept(port)`, the connection request remains pending.

`connect` blocks until the destination broker accepts the connection.

This ensures that a successfully returned channel has been accepted by both endpoints.

### 6.4.5 Successful result

When the destination accepts the connection, `connect` returns the client-side `Channel` endpoint.

The returned channel is connected and ready for reading and writing.

### 6.4.6 Multiple clients

Several tasks may call `connect` for the same broker and port.

Each successful connection must create an independent pair of channel endpoints and independent communication state.

Data sent through one connection must never be visible through another connection.

### 6.4.7 Interruption

If the calling thread is interrupted before its connection has been accepted:

1. Its pending connection request must be removed or marked as cancelled.
2. The interrupted status must be restored.
3. The method must throw `IllegalStateException` with the original `InterruptedException` as its cause.

A cancelled connection request must not later be returned by `accept`.

---

## 7. Channel lifecycle

A channel endpoint may be in one of the following logical states:

```text
CONNECTED
LOCALLY_DISCONNECTED
REMOTELY_DISCONNECTED
FULLY_DISCONNECTED
```

### 7.1 Connected

Both endpoints may read and write.

### 7.2 Locally disconnected

The local endpoint has called `disconnect`.

No new local reads or writes are permitted.

### 7.3 Remotely disconnected

The peer has called `disconnect`.

The local endpoint may still read bytes that were successfully written before the peer disconnected.

No new bytes can arrive from the disconnected peer.

### 7.4 Fully disconnected

Both endpoints have disconnected, or the local endpoint has disconnected and no further communication is permitted.

### 7.5 Initial state

A channel returned by `accept` or `connect` initially satisfies:

```java
channel.disconnected() == false
```

---

## 8. `int write(byte[] bytes, int offset, int length)`

## 8.1 Purpose

`write` transfers bytes from the caller’s array to the peer’s incoming byte stream.

The source range is:

```text
bytes[offset]
```

through:

```text
bytes[offset + length - 1]
```

---

## 8.2 Zero-length write

If `length == 0` and the arguments are otherwise valid, `write` returns immediately with:

```text
0
```

No data is transferred and the method does not block.

---

## 8.3 Normal write

When the channel is connected, `write` attempts to transfer the complete requested range.

If sufficient buffer space is not immediately available, it waits until the peer reads data and releases space.

On success, it returns:

```text
length
```

Therefore, a normal successful write satisfies:

```java
int result = channel.write(bytes, offset, length);

assert result == length;
```

---

## 8.4 Buffer-full behavior

When the outgoing communication buffer is full, `write` blocks until at least one of the following occurs:

* The peer reads bytes and creates free space.
* The local endpoint is disconnected.
* The peer endpoint is disconnected.
* The writing thread is interrupted.

The method must not use busy waiting.

---

## 8.5 Atomicity of individual bytes

Every byte is either:

* Successfully inserted into the outgoing stream exactly once, or
* Not inserted.

No byte may be duplicated or partially written.

The entire multi-byte call is not required to be atomic relative to other calls unless the single-writer rule from Section 11 is respected.

---

## 8.6 Disconnection before writing

If the local endpoint or peer endpoint is already disconnected and `length > 0`, `write` must throw:

```java
IllegalStateException
```

No byte from the requested range may be accepted.

---

## 8.7 Disconnection during writing

The method may already have placed part of the byte range in the outgoing buffer when a disconnection occurs.

If disconnection prevents completion, `write` returns the number of bytes successfully accepted before the disconnection.

The result may therefore satisfy:

```text
0 <= result < length
```

Bytes included in the returned count were successfully accepted before the disconnection.

If zero bytes were accepted and the disconnection is observed before progress is made, the method throws `IllegalStateException`.

---

## 8.8 Interruption during writing

If the writing thread is interrupted while waiting for buffer space:

1. The interrupted status must be restored.
2. Bytes already accepted remain part of the stream.
3. If at least one byte was accepted, the method returns the number accepted.
4. If no byte was accepted, the method throws `IllegalStateException` with the original `InterruptedException` as its cause.

The implementation must never roll back bytes already made visible to the peer.

---

## 8.9 FIFO guarantee

Bytes written during a successful call must become readable by the peer in the same order as they appear in the source array.

---

## 8.10 Source array ownership

`write` copies bytes from the source array.

After a byte has been accepted, later modifications to the caller’s source array must not change the byte stored in the channel.

---

## 9. `int read(byte[] bytes, int offset, int length)`

## 9.1 Purpose

`read` copies bytes from the incoming byte stream into the caller’s destination array.

Bytes are written beginning at:

```text
bytes[offset]
```

The method writes at most `length` bytes.

---

## 9.2 Zero-length read

If `length == 0` and the arguments are otherwise valid, `read` returns immediately with:

```text
0
```

The method does not wait for incoming data.

This behavior also applies when the peer has disconnected.

---

## 9.3 Read when data is available

If at least one byte is available, `read` returns immediately after copying as many currently available bytes as possible, up to `length`.

The method is not required to wait until exactly `length` bytes become available.

For a positive `length`, a normal read result satisfies:

```text
1 <= result <= length
```

For example, if the caller requests 100 bytes but only 12 bytes are currently available, the method may copy and return 12.

---

## 9.4 Empty-buffer behavior

If the incoming buffer is empty and the peer remains connected, `read` blocks until at least one of the following occurs:

* The peer writes data.
* The peer disconnects.
* The local endpoint is disconnected.
* The reading thread is interrupted.

The method must not use busy waiting.

---

## 9.5 FIFO guarantee

Bytes must be returned in the same order in which the peer successfully wrote them.

No byte may be skipped, duplicated or reordered.

---

## 9.6 Peer disconnection with buffered data

If the peer disconnects while unread bytes remain in the incoming buffer, those bytes remain readable.

`read` returns buffered bytes normally before reporting the end of the stream.

For example:

```text
Peer writes A, B and C
Peer disconnects
Local endpoint reads A, B and C
Next read returns -1
```

---

## 9.7 End of stream

If all the following conditions hold:

* The peer has disconnected.
* The incoming buffer is empty.
* `length > 0`.
* The local endpoint has not itself been explicitly disconnected.

then `read` returns:

```text
-1
```

The value `-1` represents the end of the incoming byte stream.

Once a channel returns `-1` because the peer has disconnected and no buffered bytes remain, later positive-length reads must also return `-1`, unless the local endpoint has subsequently been disconnected.

---

## 9.8 Local disconnection

After the local endpoint calls `disconnect`, a positive-length call to `read` must throw:

```java
IllegalStateException
```

A local disconnection abandons unread incoming data.

---

## 9.9 Interruption during reading

If the reading thread is interrupted while waiting for data:

1. The interrupted status must be restored.
2. No destination-array element may be modified unless data was actually read.
3. If at least one byte was read, the method returns the number of bytes read.
4. If no byte was read, the method throws `IllegalStateException` with the original `InterruptedException` as its cause.

---

## 9.10 Destination array boundaries

`read` may modify only the following destination positions:

```text
offset
```

through:

```text
offset + result - 1
```

Array elements outside this range must remain unchanged.

---

## 10. `void disconnect()`

## 10.1 Purpose

`disconnect` closes the local channel endpoint and informs the peer that no additional bytes will be produced by this endpoint.

## 10.2 Idempotence

`disconnect` is idempotent.

Calling it more than once has the same effect as calling it once.

It must not throw an exception solely because the channel is already disconnected.

## 10.3 Effect on local operations

After `disconnect` returns:

* Positive-length local writes must fail with `IllegalStateException`.
* Positive-length local reads must fail with `IllegalStateException`.
* `disconnected()` must return `true`.

## 10.4 Effect on the peer

The peer must eventually observe the disconnection.

The peer may continue reading bytes successfully written before the disconnection.

Once those bytes are exhausted, the peer’s positive-length `read` returns `-1`.

The peer must not successfully write new bytes to the disconnected endpoint.

## 10.5 Waiting operations

`disconnect` must wake all operations blocked on the channel, including:

* A local read waiting for incoming data
* A local write waiting for outgoing space
* A peer read waiting for incoming data
* A peer write waiting for outgoing space

No operation may remain blocked indefinitely after a relevant disconnection.

## 10.6 Concurrent disconnection

Both endpoints may call `disconnect` concurrently.

The final state must be fully disconnected, with no deadlock and no corrupted communication state.

---

## 11. `boolean disconnected()`

## 11.1 Purpose

`disconnected` reports whether the local endpoint can still be used for normal bidirectional communication.

## 11.2 Result

The method returns `true` if either endpoint has disconnected.

It returns `false` only while both endpoints remain connected.

Therefore, if the peer disconnects, the local endpoint eventually observes:

```java
channel.disconnected() == true
```

even though unread buffered bytes may still be retrieved with `read`.

## 11.3 Non-blocking behavior

`disconnected` must return immediately.

It must not wait for communication activity.

## 11.4 Visibility

The result must reflect disconnection performed by another thread without requiring unrelated synchronization from the caller.

---

## 12. Concurrency model

## 12.1 Permitted concurrency

Different channel connections may be used concurrently.

The two directions of the same connection may also be used concurrently. For example:

* Task A may write while Task B writes.
* Task A may read while Task A writes.
* Task A may read while Task B writes.

## 12.2 Single-writer rule

At most one thread may call `write` on a particular channel endpoint at a time.

The implementation is not required to preserve separate call boundaries when multiple threads concurrently write through the same endpoint.

## 12.3 Single-reader rule

At most one thread may call `read` on a particular channel endpoint at a time.

The implementation is not required to support several threads concurrently consuming the same incoming stream.

## 12.4 Disconnection concurrency

`disconnect` and `disconnected` may be called concurrently with `read` or `write`.

These calls must be thread-safe.

## 12.5 Deadlock freedom

Normal channel operations must not deadlock when used according to the single-reader and single-writer rules.

In particular:

* A blocked reader must wake when data arrives or disconnection occurs.
* A blocked writer must wake when space becomes available or disconnection occurs.
* Two concurrent disconnections must not deadlock.

## 12.6 Busy waiting

The implementation must not repeatedly poll shared state in a tight loop.

Waiting must use an appropriate blocking mechanism, such as:

* `wait` and `notifyAll`
* `Condition`
* A blocking queue
* The event mechanism supplied by the project framework

---

## 13. Circular-buffer contract

The local channel implementation uses circular buffers.

Each circular buffer stores bytes in FIFO order.

## 13.1 Construction

A circular buffer is created with a fixed internal array size.

The array size must be at least two.

If the implementation distinguishes the full and empty states by leaving one array position unused, an internal array of length `n` has the effective capacity:

```text
n - 1
```

## 13.2 Empty state

The buffer is empty when it contains no readable byte.

Immediately after construction:

```java
buffer.empty() == true
```

and:

```java
buffer.full() == false
```

## 13.3 Full state

The buffer is full when no additional byte can be inserted without first removing a byte.

## 13.4 `push`

`push` inserts one byte at the end of the FIFO sequence.

If the buffer is full, it throws:

```java
IllegalStateException
```

A failed `push` must not change the buffer.

## 13.5 `pull`

`pull` removes and returns the oldest byte.

If the buffer is empty, it throws:

```java
IllegalStateException
```

A failed `pull` must not change the buffer.

## 13.6 Wrap-around

The logical order of stored bytes must remain correct when the head or tail index reaches the end of the backing array and wraps to the beginning.

## 13.7 Concurrency assumption

One circular buffer supports exactly:

* One producer
* One consumer

The producer is the only thread that advances the write position.

The consumer is the only thread that advances the read position.

Using several producers or several consumers on the same circular buffer is outside its contract unless additional synchronization is added by the channel layer.

---

## 14. Task specification

## 14.1 `Task(Broker broker, Runnable runnable)`

### Purpose

The constructor creates a task that executes `runnable` in a thread associated with `broker`.

### Preconditions

Neither argument may be `null`.

If `broker` is `null`, the constructor must throw:

```java
NullPointerException
```

If `runnable` is `null`, the constructor must throw:

```java
NullPointerException
```

### Initial state

Constructing a task does not start it.

The task begins execution only after:

```java
task.start();
```

### Broker association

Before invoking the supplied runnable, the task associates the current thread with the supplied broker.

This association remains valid for the duration of the runnable’s execution.

The association must be removed when the runnable terminates, including when it terminates because of an exception.

---

## 14.2 `static Broker getBroker()`

### Purpose

`getBroker` returns the broker associated with the currently executing task.

### Normal result

When called from the runnable of a `Task`, it returns the same broker that was supplied to the task’s constructor.

Example:

```java
Broker expected = ...;

Task task = new Task(expected, () -> {
    assert Task.getBroker() == expected;
});
```

### Outside a task

If the current thread is not executing as a `Task`, `getBroker` must throw:

```java
IllegalStateException
```

It must not return an unrelated broker.

### Multiple tasks

Different tasks may be associated with different brokers.

Calling `getBroker` concurrently from those tasks must return the correct broker for each task.

---

## 15. Error handling summary

| Situation                                               | Required behavior                                   |
| ------------------------------------------------------- | --------------------------------------------------- |
| Null byte array                                         | `NullPointerException`                              |
| Invalid offset or length                                | `IndexOutOfBoundsException`                         |
| Invalid broker name                                     | `IllegalArgumentException`                          |
| Duplicate broker name                                   | `IllegalArgumentException`                          |
| Invalid port                                            | `IllegalArgumentException`                          |
| Unknown destination broker                              | `IllegalArgumentException`                          |
| Concurrent `accept` on the same port                    | `IllegalStateException`                             |
| Positive-length read after local disconnect             | `IllegalStateException`                             |
| Positive-length write after either endpoint disconnects | `IllegalStateException`                             |
| Read after peer disconnects and buffer is empty         | Return `-1`                                         |
| Zero-length read or write                               | Return `0`                                          |
| Pull from empty circular buffer                         | `IllegalStateException`                             |
| Push into full circular buffer                          | `IllegalStateException`                             |
| `Task.getBroker()` outside a task                       | `IllegalStateException`                             |
| Interruption while blocked with no progress             | Restore interrupt and throw `IllegalStateException` |

---

## 16. Required behavioral examples

## 16.1 Basic one-way communication

Given two brokers:

```text
client
server
```

and the server operation:

```java
Channel serverChannel = serverBroker.accept(5000);
```

the client may connect using:

```java
Channel clientChannel =
    clientBroker.connect("server", 5000);
```

If the client writes:

```java
byte[] message = {10, 20, 30};

clientChannel.write(message, 0, message.length);
```

the server must be able to read:

```text
10, 20, 30
```

in that order.

---

## 16.2 Bidirectional communication

After connection:

```text
Client writes "request"
Server reads "request"

Server writes "response"
Client reads "response"
```

Both directions belong to the same logical connection but use independent byte streams.

---

## 16.3 Partial read

Suppose the client writes five bytes:

```text
1, 2, 3, 4, 5
```

The server may read them using:

```java
byte[] first = new byte[2];
byte[] second = new byte[3];

int n1 = channel.read(first, 0, 2);
int n2 = channel.read(second, 0, 3);
```

The complete observed sequence must remain:

```text
1, 2, 3, 4, 5
```

---

## 16.4 Large transfer

A write larger than the circular-buffer capacity must still succeed while the peer concurrently reads.

The writer may block temporarily while the buffer is full.

The implementation must eventually transfer the entire requested range as long as:

* The peer continues reading.
* Neither endpoint disconnects.
* Neither thread is interrupted.

---

## 16.5 Disconnection after writing

The following sequence is valid:

```text
A writes X, Y and Z
A disconnects
B reads X, Y and Z
B reads again
B receives -1
```

The disconnection must not discard the bytes already written.

---

## 16.6 Disconnection while reading

If B is blocked in `read` because no data is available and A disconnects, B must wake and return:

```text
-1
```

provided B has not locally disconnected and no unread byte remains.

---

## 16.7 Disconnection while writing

If A is blocked because its outgoing buffer is full and B disconnects, A must wake.

A must not remain blocked indefinitely.

If A had already transferred some bytes, `write` returns the number transferred. Otherwise, it throws `IllegalStateException`.

---

## 17. Required tests

The implementation must be tested at four levels.

### 17.1 Circular-buffer unit tests

Tests must cover:

* Initial empty state
* FIFO behavior
* Full state
* Empty-buffer failure
* Full-buffer failure
* Wrap-around
* Repeated push and pull operations
* Effective capacity

### 17.2 Broker tests

Tests must cover:

* Broker registration
* Duplicate broker names
* Invalid broker names
* Invalid ports
* Unknown destination
* `accept` before `connect`
* `connect` before `accept`
* Multiple connections
* Different ports
* FIFO ordering of pending connections
* Interruption of `accept`
* Interruption of `connect`

### 17.3 Channel tests

Tests must cover:

* One-byte transfer
* Multi-byte transfer
* Bidirectional transfer
* FIFO ordering
* Nonzero offsets
* Zero-length operations
* Invalid arguments
* Transfers larger than the buffer
* Blocking read
* Blocking write
* Peer disconnection
* Local disconnection
* Buffered data after peer disconnection
* Repeated disconnection
* Disconnection while blocked
* Concurrent read and write in opposite directions

### 17.4 Task tests

Tests must cover:

* Correct broker association
* Different brokers for different tasks
* `getBroker` outside a task
* Removal of task association after termination
* Association cleanup when the runnable throws an exception

### 17.5 Test termination

Concurrency tests must have a finite timeout.

A deadlock must cause a test failure instead of blocking the complete test suite indefinitely.

---

## 18. Correctness properties

A conforming implementation must satisfy the following properties.

### Safety properties

1. No successfully written byte is silently corrupted.
2. No successfully written byte is duplicated.
3. Bytes are observed in FIFO order.
4. Data from independent connections is never mixed.
5. Array elements outside the requested read range are not modified.
6. A full circular buffer is never overwritten.
7. An empty circular buffer is never read.
8. A disconnected endpoint cannot resume normal communication.

### Liveness properties

1. A waiting reader eventually proceeds when data becomes available.
2. A waiting writer eventually proceeds when buffer space becomes available.
3. Waiting operations wake when a relevant endpoint disconnects.
4. A connection request eventually completes when a matching accept operation is executed.
5. Operations do not deadlock when the API is used according to its concurrency contract.

These liveness properties assume that the Java runtime continues scheduling the relevant threads.

---

## 19. Explicit limitations

The first version does not provide:

* Communication between different JVM processes
* Communication across a network
* Message boundaries
* Encryption
* Authentication
* Byzantine-failure protection
* Automatic reconnection
* Failure detection based on heartbeats or timeouts
* Multiple simultaneous producers on one circular buffer
* Multiple simultaneous consumers on one circular buffer
* Guaranteed fairness between competing threads
* Persistence after JVM termination

These features may be introduced in later versions without changing the fundamental purpose of the `Broker`, `Channel` and `Task` abstractions.

---

## 20. Compliance

An implementation conforms to this specification when:

1. It preserves the public API.
2. It satisfies the defined normal behavior.
3. It applies the specified error behavior.
4. It respects FIFO and lossless stream semantics.
5. It correctly handles blocking and disconnection.
6. It passes the required unit, integration, concurrency and stress tests.
7. Its behavior does not depend on timing assumptions that are absent from this specification.
