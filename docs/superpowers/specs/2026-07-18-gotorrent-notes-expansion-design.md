# GoTorrent Notes Expansion Design

## Objective

Upgrade the Obsidian vault at `C:\Users\karth\Documents\Obsidian Vault\GoTorrent` into a detailed, implementation-grounded guide that helps the author both understand GoTorrent and explain it confidently in technical interviews.

The notes must describe the current repository behavior rather than only presenting BitTorrent theory. Each important subsystem will connect protocol concepts to concrete Go types, functions, state, goroutines, synchronization boundaries, failure paths, and engineering tradeoffs.

## Audience and Success Criteria

The primary audience is the project author preparing to explain the system in interviews. A successful note set lets the reader:

- trace a torrent from CLI startup through parsing, discovery, downloading, verification, persistence, reannounce, and shutdown;
- identify which component owns each mutable state value;
- explain why channels, mutexes, atomics, contexts, and single-writer patterns are used;
- reason about partial failure in trackers, peers, WebSeeds, files, and resume state;
- answer follow-up questions about concurrency, distributed systems, and networking using examples from the repository;
- distinguish current behavior from possible future improvements.

## Chosen Organization

Use a hybrid structure: expand all existing notes and add three focused deep dives. This avoids leaving old notes stale while preventing concurrency and networking concepts from being duplicated across every subsystem note.

### Existing Notes to Expand

- `README.md`: vault map, learning order, current feature inventory, and complete runtime overview.
- `Architecture.md`: package boundaries, dependency direction, ownership model, lifecycle, and end-to-end control/data flows.
- `Torrent Parsing.md`: bencode parsing, raw-info extraction, validation invariants, multi-file layout, and infohash correctness.
- `Tracker.md`: concurrent discovery, HTTP/UDP abstraction, two-phase startup fallback, interval merging, deduplication, and error semantics.
- `UDP.md`: BEP 15 connect/announce packets, transaction IDs, connection expiry, deadlines, response validation, and UDP failure modes.
- `Peer.md`: TCP handshake, stream framing, reader/writer goroutines, choke state, message routing, shutdown, and live connection setup.
- `Piece.md`: selection, claiming, block assembly, verification, collection, writing, upload, progress metrics, and resume behavior.
- `WebSeed.md`: BEP 17/19 mapping, range requests, multi-file splitting, scheduling, backoff, disabling, and shared integrity path.
- `GoTorrent Interview Questions.md`: expanded project-specific interview bank with expected answers and likely follow-ups.

### New Deep-Dive Notes

- `Concurrency and Synchronization.md`: goroutine topology, ownership, locks, channels, atomics, idempotent close, backpressure, races, and cancellation.
- `Session and Reannounce.md`: startup events, periodic/manual reannounce, shared cooldown, tracker-derived intervals, failure retry, known-peer filtering, connection, and live manager injection.
- `Networking Flows.md`: protocol layering and sequence diagrams for tracker discovery, UDP packets, peer handshake/wire messages, block transfer, WebSeed requests, timeouts, and retries.

## Required Explanation Pattern

Every major implementation section should answer these questions:

1. What problem does this component solve?
2. Where is it implemented?
3. What inputs and state does it receive?
4. Which goroutine or component owns that state?
5. What happens step by step on the success path?
6. What happens on timeout, cancellation, corrupt input, or partial failure?
7. Which invariant makes the behavior safe or correct?
8. Why was this design chosen, and what are its tradeoffs?
9. How should the author explain it concisely in an interview?
10. What follow-up question is an interviewer likely to ask?

Code snippets will be short and illustrative. Function and file references will point readers to the implementation without copying large sections of source.

## Core Flows to Document

### Startup and Source Discovery

Trace `cmd/gotorrent/main.go` from argument parsing and signal-aware context creation through torrent parsing, peer-ID generation, manager construction, verified resume loading, tracker discovery, peer connection, WebSeed fallback, download startup, and cleanup.

Explain the bounded discovery policy:

- all trackers are queried concurrently;
- the first phase has a three-second budget;
- tracker results are candidate addresses, not live connections;
- a ten-second fallback occurs only when no peer connects and no WebSeed is available;
- peers from both phases are merged and deduplicated;
- the retry count is bounded to avoid indefinite startup.

### Tracker Reannounce and Live Peer Injection

Trace `internal/session` through timer/manual wake-up, construction of an event-less announce using current verified `downloaded` and `left`, ten-second request timeout, tracker interval selection, failure retry after five minutes, filtering of previously attempted addresses, peer connection, and `Manager.AddPeers`.

Document the `ReannounceController` mutex-protected state, non-blocking buffered request channel, shared one-minute cooldown, and in-flight serialization.

### Peer Connection and Wire Protocol

Explain TCP stream semantics, fixed handshake layout, `io.ReadFull`, infohash validation, optional initial bitfield, reader and single-writer goroutines, control/data message routing, choke/unchoke behavior, deadlines, channel backpressure, context cancellation, and idempotent shutdown.

### Piece Download and Integrity Pipeline

Trace selection and claim under synchronization, block requests, unique payload accounting, in-memory piece assembly, SHA1 verification, result collection, disk writing, completion marking, `have` broadcasts, and retry release.

Emphasize that the collector is the completion authority and that peer/WebSeed data passes through the same verification and persistence path.

### Resume Reliability

Explain infohash-scoped state paths, completion bitmap persistence, reload validation, rehashing allegedly complete pieces from disk, resetting stale pieces, legacy state migration, periodic save, shutdown save, and resume deletion after successful completion.

State the concurrency boundary: separate payload directories are supported across processes; simultaneous writers to the same payload directory are not.

### Metrics and TUI

Distinguish verified completion bytes from transferred payload bytes. Explain why the first drives progress and remaining work while the atomic transfer counter feeds a rolling three-second rate and ETA. Cover immediate TUI startup, background network work, synchronized snapshots, manual `r` reannounce, `q` cancellation, and terminal result delivery.

## Interview Guide Design

Expand the guide to approximately 50–60 questions grouped by:

- architecture and boundaries;
- Go concurrency and synchronization;
- TCP and wire-protocol correctness;
- HTTP/UDP tracker networking;
- distributed discovery and partial failure;
- scheduling, endgame, and integrity;
- storage, multi-file writes, and resume correctness;
- WebSeed behavior;
- observability, testing, and race detection;
- limitations and system-design extensions.

Each question will include:

- how an interviewer may phrase it;
- a direct expected answer grounded in GoTorrent;
- a deeper technical explanation;
- one or more likely follow-up questions;
- pitfalls or weak answers to avoid where useful.

The guide will include scenario questions such as a slow tracker, duplicate peers, a choked peer, a partial TCP frame, corrupt WebSeed data, a piece spanning files, cancellation during a write, stale resume state, peer injection during active scheduling, and concurrent outbound writes.

## Diagrams and Navigation

Use Obsidian-compatible Mermaid diagrams only where they materially improve understanding:

- overall component and lifecycle flow;
- tracker discovery and fallback sequence;
- periodic/manual reannounce sequence;
- peer reader/writer goroutine topology;
- piece state transition and integrity flow;
- cancellation and shutdown propagation.

Add bidirectional wiki links between the overview, subsystem notes, deep dives, and interview guide. Avoid duplicating full explanations; subsystem notes provide concrete implementation detail while deep dives compare cross-cutting behavior.

## Accuracy and Verification

Implementation claims will be checked against the current Go source and tests. The update will preserve valid existing content but correct stale statements, including missing `internal/session` coverage and older resume-state descriptions.

After editing:

- scan for broken wiki-link targets;
- scan for placeholder text and malformed Markdown headings;
- confirm all implementation references exist;
- compare documented constants and timeouts with the code;
- review diffs to ensure the vault remains readable and internally consistent.

## Scope Boundaries

The notes may discuss DHT, PEX, magnets, BitTorrent v2, uTP, rate limiting, and stronger crash consistency as interview extensions, but they must clearly label these as unimplemented. The task does not change GoTorrent source code or behavior.
