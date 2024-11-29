//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"

namespace chfs {

/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
 public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
 public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
};

enum class LogEntryType { INVALID, BLOCK_CHANGE, COMMIT, INVALID_BLOCK_CHANGE };

struct LogEntry {
  block_id_t block_id;
  txn_id_t txn_id;
  LogEntryType type;
  u8 block_data[0];

  auto tell_size() -> usize {
    switch (this->type) {
      case LogEntryType::BLOCK_CHANGE:
      case LogEntryType::INVALID_BLOCK_CHANGE: {
        return sizeof(LogEntry)+DiskBlockSize;
      }
      default:
        return sizeof(LogEntry);
    }
  }
};

struct PackedLogEntry {
  block_id_t block_id;
  txn_id_t txn_id;
  LogEntryType type;

  std::vector<u8> block_data;
  PackedLogEntry(const LogEntry &log_entry, const std::vector<u8> &data_vec)
      : block_id(log_entry.block_id),
        txn_id(log_entry.txn_id),
        type(log_entry.type),
        block_data(data_vec) {
    if (type == LogEntryType::INVALID_BLOCK_CHANGE)
      type = LogEntryType::INVALID;
  }
  PackedLogEntry(block_id_t block_id, LogEntryType log_entry_type,
                 txn_id_t txn_id, const std::vector<u8> &data)
      : block_id(block_id),
        txn_id(txn_id),
        type(log_entry_type),
        block_data(data) {
    if (type == LogEntryType::INVALID_BLOCK_CHANGE)
      type = LogEntryType::INVALID;
  }
  auto to_vector() -> std::vector<u8> {
    LogEntry logEntry{block_id, txn_id, type, {}};
    auto result = std::vector<u8>(logEntry.tell_size());

    auto entry_data = result.data();
    auto result_p = reinterpret_cast<LogEntry *>(entry_data);
    result_p->type = this->type;
    result_p->block_id = this->block_id;
    result_p->txn_id = this->txn_id;
    result_p->type = this->type;
    if (this->type == LogEntryType::BLOCK_CHANGE) {
      memcpy(result_p->block_data, this->block_data.data(), DiskBlockSize);
    }
    return result;
  }
};

}  // namespace chfs