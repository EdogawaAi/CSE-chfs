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
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  return bm_->get_log_entries().size();
  return 0;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  for(const auto &op:ops){
    PackedLogEntry packed_entry(op->block_id_,LogEntryType::BLOCK_CHANGE,txn_id,op->new_block_state_);
    auto entry_vec=packed_entry.to_vector();
    this->bm_->write_log_entry(entry_vec);
  }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  PackedLogEntry packed_entry(0,LogEntryType::COMMIT,txn_id,std::vector<u8>());
  this->bm_->write_log_entry(packed_entry.to_vector());
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  // This implementation is incorrect.
  // However, since we have to lock the inode table
  // when doing mknode and unlink, there is only one concurrent tx.
  this->bm_->reset_logging_area();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  auto entries=this->bm_->get_log_entries();
  for(const auto &entry:entries){
    if(entry.type==LogEntryType::BLOCK_CHANGE){
      this->bm_->write_partial_block_wo_failure(entry.block_id,entry.block_data.data(),0,this->bm_->block_size());
    }
  }
}
}; // namespace chfs