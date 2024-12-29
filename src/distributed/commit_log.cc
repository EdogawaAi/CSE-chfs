#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm), log_current_offset(0), num_tx_in_log(0), current_txn_id(1){
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  return num_tx_in_log;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  //! log_mtx //
  log_mtx.lock();

  num_tx_in_log++;
  auto initOffset = log_current_offset;

  for (auto &op : ops)
  {
    LogEntry log_entry;
    log_entry.txn_id = txn_id;
    log_entry.block_id = op->block_id_;
    std::vector<u8> buffer(sizeof(LogEntry) + DiskBlockSize);
    log_entry.flush_to_buffer(buffer.data());
    auto logentryArray = reinterpret_cast<LogEntry *>(buffer.data());
    for (int i = 0; i < DiskBlockSize; i++)
    {
      logentryArray->new_block_state[i] = op->new_block_state_[i];
    }
    bm_->write_log_entry(log_current_offset, buffer.data(), sizeof(LogEntry) + DiskBlockSize);
    log_current_offset += sizeof(LogEntry) + DiskBlockSize;
  }

  auto startBlockId = initOffset / bm_->block_size();
  auto endBlockId = log_current_offset / bm_->block_size();
  const auto baseBlockId = bm_->total_blocks();
  for (auto i = startBlockId; i < endBlockId; i++)
  {
    bm_->sync(i + baseBlockId);
  }

  commit_log(txn_id);

  if (is_checkpoint_enabled_)
  {
    if (num_tx_in_log >= 100 || log_current_offset >= DiskBlockSize * 1000)
    {
      checkpoint();
    }
  }

  log_mtx.unlock();
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto initialOffset = log_current_offset;
  LogEntry log_entry;
  log_entry.txn_id = txn_id;
  log_entry.block_id = 0xFFFFFFFFFFFFFFFF;
  std::vector<u8> buffer(sizeof(LogEntry) + DiskBlockSize);
  log_entry.flush_to_buffer(buffer.data());
  bm_->write_log_entry(log_current_offset, buffer.data(), sizeof(LogEntry) + DiskBlockSize);
  log_current_offset += sizeof(LogEntry) + DiskBlockSize;

  auto startBlockId = initialOffset / bm_->block_size();
  auto endBlockId = log_current_offset / bm_->block_size();
  const auto baseBlockId = bm_->total_blocks();
  for (auto i = startBlockId; i < endBlockId; i++)
  {
    bm_->sync(i + baseBlockId);
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto logStart = bm_->get_log_start();
  memset(logStart, 0, DiskBlockSize * 1024);
  log_current_offset = 0;
  num_tx_in_log = 0;

  bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto logStart = bm_->get_log_start();
  const auto logEnd = bm_->get_log_end();
  auto iter = logStart;
  while (iter < logEnd)
  {
    auto log_entry_ptr = reinterpret_cast<LogEntry *>(iter);
    if (log_entry_ptr->txn_id == 0)
    {
      break;
    }

    auto numEntryTx = 0;
    auto thisTxnId = log_entry_ptr->txn_id;
    bool isThisTxCommitted = false;
    auto txnIter = iter;
    while (txnIter < logEnd)
    {
      numEntryTx++;
      auto logentryArray = reinterpret_cast<LogEntry *>(txnIter);
      if (logentryArray->txn_id != thisTxnId)
      {
        break;
      }
      if (logentryArray->block_id == 0xFFFFFFFFFFFFFFFF)
      {
        isThisTxCommitted = true;
        break;
      }
      txnIter += (sizeof(LogEntry) + DiskBlockSize);
    }

    if (!isThisTxCommitted)
    {
      iter += (sizeof(LogEntry) + DiskBlockSize) * numEntryTx;
      continue;
    }

    auto redoIter = iter;
    while (redoIter < txnIter)
    {
      auto logentryArray = reinterpret_cast<LogEntry *>(redoIter);
      auto blockId = logentryArray->block_id;
      auto newBlockState = logentryArray->new_block_state;
      std::vector<u8> buffer(DiskBlockSize);
      for (int i = 0; i < DiskBlockSize; ++i)
      {
        buffer[i] = newBlockState[i];
      }
      bm_->write_block_for_recover(blockId, buffer.data());
      redoIter += (sizeof(LogEntry) + DiskBlockSize);
    }
    iter += (sizeof(LogEntry) + DiskBlockSize) * numEntryTx;
  }
}
}; // namespace chfs