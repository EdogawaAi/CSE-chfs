//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/block/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "common/result.h"

namespace chfs {
// TODO

class BlockIterator;
class BlockOperation;
struct PackedLogEntry;

/**
 * BlockManager implements a block device to read/write block devices
 * Note that the block manager is **not** thread-safe.
 */
class BlockManager {
  friend class BlockIterator;

 protected:
  const usize block_sz = 4096;

  std::string file_name_;
  int fd;
  u8 *block_data;
  usize block_cnt;
  bool in_memory;  // whether we use in-memory to emulate the block manager
  bool maybe_failed;
  usize write_fail_cnt;

  const usize BLOCKS_RESERVED_FOR_LOGGING = 1024;
  const usize OFFSET_OF_LOGGING_AREA = 2 * sizeof(usize) + sizeof(txn_id_t);
  const usize OFFSET_OF_MAX_TXN_ID = 2 * sizeof(usize);

  bool log_enabled = false;
  std::vector<std::shared_ptr<BlockOperation>> *now_logging_vec = nullptr;
  std::mutex txn_id_mutex;

 public:
  /**
   * Creates a new block manager that writes to a file-backed block device.
   * @param block_file the file name of the  file to write to
   */
  explicit BlockManager(const std::string &file);

  /**
   * Creates a new block manager that writes to a file-backed block device.
   * @param block_file the file name of the  file to write to
   * @param block_cnt the number of expected blocks in the device. If the
   * device's blocks are more or less than it, the manager should adjust the
   * actual block cnt.
   */
  BlockManager(const std::string &file, usize block_cnt);

  /**
   * Creates a memory-backed block manager that writes to a memory block device.
   * Note that this is commonly used for testing.
   * Maybe it can be used for non-volatile memory, but who knows.
   *
   * @param block_count the number of blocks in the device
   * @param block_size the size of each block
   */
  BlockManager(usize block_count, usize block_size);

  /**
   * Creates a new block manager that writes to a file-backed block device.
   * It reserves some blocks for recording logs.
   *
   * @param block_file the file name of the  file to write to
   * @param block_cnt the number of blocks in the device
   * @param is_log_enabled whether to enable log
   */
  BlockManager(const std::string &file, usize block_cnt, bool is_log_enabled);

  virtual ~BlockManager();

  /**
   * Write a block to the internal block device.  This is a write-through one,
   * i.e., no cache.
   * @param block_id id of the block
   * @param block_data raw block data
   */
  virtual auto write_block(block_id_t block_id, const u8 *block_data)
      -> ChfsNullResult;

  /**
   * Write a partial block to the internal block device.
   */
  virtual auto write_partial_block(block_id_t block_id, const u8 *block_data,
                                   usize offset, usize len) -> ChfsNullResult;

  /**
   * Read a block to the internal block device.
   * @param block_id id of the block
   * @param block_data raw block data buffer to store the result
   */
  virtual auto read_block(block_id_t block_id, u8 *block_data)
      -> ChfsNullResult;

  /**
   * Clear the content of a block
   * @param block_id id of the block
   */
  virtual auto zero_block(block_id_t block_id) -> ChfsNullResult;

  auto total_storage_sz() const -> usize {
    return this->block_cnt * this->block_sz;
  }

  auto total_storage_sz_w_log() const -> usize {
    return (this->block_cnt + this->BLOCKS_RESERVED_FOR_LOGGING) *
           this->block_sz;
  }

  /**
   * Get the total number of blocks in the block manager
   */
  auto total_blocks() const -> usize { return this->block_cnt; }

  /**
   * Get the block size of the device managed by the manager
   */
  auto block_size() const -> usize { return this->block_sz; }

  /**
   * Get the block data pointer of the manager
   */
  auto unsafe_get_block_ptr() const -> u8 * { return this->block_data; }

  /**
   * flush the data of a block into disk
   */
  auto sync(block_id_t block_id) -> ChfsNullResult;

  /**
   * Flush the page cache
   */
  auto flush() -> ChfsNullResult;

  /**
   * Mark the block manager as may fail state
   */
  auto set_may_fail(bool may_fail) -> void { this->maybe_failed = may_fail; }

  auto read_bytes_from_log_area(usize start_byte_idx, usize length)
      -> std::vector<u8>;

  auto write_bytes_to_log_area(usize start_byte_idx, std::vector<u8> data)
      -> bool;

  auto get_log_area_start_end_byte_idx() -> std::pair<usize, usize>;

  // if some value is 0, don't update it
  auto set_log_area_start_end_byte_idx(usize start, usize end) -> bool;

  auto write_log_entry(std::vector<u8> entry_vec) -> bool;

  auto get_log_entries() -> std::vector<PackedLogEntry>;

  auto write_partial_block_wo_failure(block_id_t block_id, const u8 *data,
                                      usize offset, usize len)
      -> ChfsNullResult;

  auto start_logging(std::vector<std::shared_ptr<BlockOperation>> *log_vec)
      -> ChfsNullResult;

  auto stop_logging() -> ChfsNullResult;

  auto flush_ops(std::vector<std::shared_ptr<BlockOperation>> *log_vec)
      -> ChfsNullResult;

  void set_max_txn_id(txn_id_t max_txn_id);

  auto increase_and_get_txn_id() -> txn_id_t;

  void reset_logging_area();
};

/**
 * A class to simplify iterating blocks in the block manager.
 *
 * Note that we don't provide a conventional iterator interface, because
 * each block read/write may return error due to failed reading/writing blocks.
 */
class BlockIterator {
  BlockManager *bm;
  u64 cur_block_off;
  block_id_t start_block_id;
  block_id_t end_block_id;

  std::vector<u8> buffer;

 public:
  /**
   * Creates a new block iterator.
   *
   * @param bm the block manager to iterate
   * @param start_block_id the start block id of the iterator
   * @param end_block_id the end block id of the iterator
   */
  static auto create(BlockManager *bm, block_id_t start_block_id,
                     block_id_t end_block_id) -> ChfsResult<BlockIterator>;

  /**
   * Iterate to the cur_block_off to an offset
   *
   * **Assumption**: a previous call of has_next() returns true
   *
   * @param offset the offset to iterate to
   *
   * @return Ok(the iterator itself)
   *         Err(DONE) // the iteration is done, i.e., we have passed the
   * end_block_id Other errors
   */
  auto next(usize offset) -> ChfsNullResult;

  auto has_next() -> bool {
    return this->cur_block_off <
           (this->end_block_id - this->start_block_id) * bm->block_sz;
  }

  /**
   *  Assumption: a prior call of has_next() must return true
   */
  auto flush_cur_block() -> ChfsNullResult {
    auto target_block_id =
        this->start_block_id + this->cur_block_off / bm->block_sz;
    return this->bm->write_block(target_block_id, this->buffer.data());
  }

  auto get_cur_byte() const -> u8 {
    return this->buffer[this->cur_block_off % bm->block_sz];
  }

  template <typename T>
  auto unsafe_get_value_ptr() -> T * {
    return reinterpret_cast<T *>(this->buffer.data() +
                                 this->cur_block_off % bm->block_sz);
  }
};

}  // namespace chfs
