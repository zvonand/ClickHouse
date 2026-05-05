#pragma once

#include "config.h"

namespace DB::JemallocPartsArena
{

/// Returns the jemalloc arena index dedicated to MergeTree per-part metadata
/// (the per-part `NamesAndTypesList`, `SerializationInfoByName`, `ColumnsSubstreams`,
///  `MergeTreeDataPartChecksums` tree, and the `Poco::LRUCache<String, ColumnSize>`
///  delegates inside `IMergeTreeDataPart`).
///
/// Creates the arena on first call (thread-safe via Meyers singleton).
/// Returns 0 (meaning "use default arena selection") if jemalloc is not available.
///
/// LLVM-style: callers route allocations into this arena for a tightly-bounded scope by
/// using `ScopedJemallocThreadArena` from `Common/Jemalloc.h`. Frees auto-route via
/// jemalloc's per-extent metadata, so only allocation paths need scoping.
unsigned getArenaIndex();

/// Whether the dedicated parts arena is available (jemalloc compiled in).
bool isEnabled();

/// Purge dirty pages only in the parts arena, returning memory to the OS.
/// No-op if jemalloc is not available.
void purge();

}
