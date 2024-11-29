#include "block/manager.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstddef>
#include <cstring>
#include <filesystem>

#include "common/result.h"
#include "distributed/commit_log.h"

namespace chfs {

auto get_file_sz(std::string &file_name) -> usize {
  std::filesystem::path path = file_name;
  return std::filesystem::file_size(path);
}

/**
 * If the opened block manager's backed file is empty,
 * we will initialize it to a pre-defined size.
 */
auto initialize_file(int fd, u64 total_file_sz) {
  if (ftruncate(fd, total_file_sz) == -1) {
    CHFS_ASSERT(false, "Failed to initialize the block manager file");
  }
}

/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file)
    : BlockManager(file, KDefaultBlockCnt) {}

/**
 * Creates a new block manager that writes to a file-backed block device.
 * @param block_file the file name of the  file to write to
 * @param block_cnt the number of expected blocks in the device. If the
 * device's blocks are more or less than it, the manager should adjust the
 * actual block cnt.
 */
BlockManager::BlockManager(usize block_cnt, usize block_size)
    : block_sz(block_size),
      file_name_("in-memory"),
      fd(-1),
      block_cnt(block_cnt),
      in_memory(true) {
  // An important step to prevent overflow
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  u64 buf_sz = static_cast<u64>(block_cnt) * static_cast<u64>(block_size);
  CHFS_VERIFY(buf_sz > 0, "Santiy check buffer size fails");
  this->block_data = new u8[buf_sz];
  CHFS_VERIFY(this->block_data != nullptr, "Failed to allocate memory");
}

/**
 * Core constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file, usize block_cnt)
    : file_name_(file), block_cnt(block_cnt), in_memory(false) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  if (file_sz == 0) {
    initialize_file(this->fd, this->total_storage_sz_w_log());
  } else {
    this->block_cnt = file_sz / this->block_sz - BLOCKS_RESERVED_FOR_LOGGING;
    CHFS_ASSERT(this->block_cnt = KDefaultBlockCnt, "The file size mismatches");
  }

  this->block_data =
      static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                             PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
  CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");
}

BlockManager::BlockManager(const std::string &file, usize block_cnt,
                           bool is_log_enabled)
    : file_name_(file), block_cnt(block_cnt), in_memory(false) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  bool will_init_log = false;
  if (is_log_enabled) {
    if (file_sz == 0) {
      initialize_file(this->fd, this->total_storage_sz_w_log());
      will_init_log = true;
    } else {
      this->block_cnt = file_sz / this->block_sz - BLOCKS_RESERVED_FOR_LOGGING;
      CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                  "The file size mismatches");
    }
    this->block_data = static_cast<u8 *>(mmap(
        nullptr,
        this->total_storage_sz() + BLOCKS_RESERVED_FOR_LOGGING * this->block_sz,
        PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
    if (will_init_log)
      this->set_log_area_start_end_byte_idx(OFFSET_OF_LOGGING_AREA,
                                            OFFSET_OF_LOGGING_AREA + 1);
    this->set_max_txn_id(0);
    this->log_enabled = true;
  } else {
    if (file_sz == 0) {
      initialize_file(this->fd, this->total_storage_sz_w_log());
    } else {
      this->block_cnt = file_sz / this->block_sz - BLOCKS_RESERVED_FOR_LOGGING;
      CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                  "The file size mismatches");
    }
    this->block_data = static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED, this->fd, 0));
  }

  CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");
}

auto BlockManager::write_block(block_id_t block_id, const u8 *data)
    -> ChfsNullResult {
  if (this->now_logging_vec) {
    auto siz = this->block_size();
    auto new_block_data_log = std::vector<u8>(this->block_size());
    for (u64 i = 0; i < siz; i++) {
      new_block_data_log[i] = data[i];
    }
    this->now_logging_vec->push_back(
        std::make_shared<BlockOperation>(block_id, new_block_data_log));
    return KNullOk;
  }
  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      return ErrorType::INVALID;
    }
  }

  u64 start_byte_index = block_id * this->block_size();
  for (u64 block_offset = 0; block_offset < this->block_size();
       block_offset++) {
    this->block_data[start_byte_index + block_offset] = data[block_offset];
  }

  this->write_fail_cnt++;
  return KNullOk;
}

auto BlockManager::write_partial_block(block_id_t block_id, const u8 *data,
                                       usize offset, usize len)
    -> ChfsNullResult {
  if (this->now_logging_vec) {
    auto siz = this->block_size();
    auto new_block_data_log = std::vector<u8>(this->block_size());
    u64 start_byte_index = block_id * this->block_size();
    for (u64 block_offset = 0; block_offset < siz; block_offset++) {
      new_block_data_log[block_offset] =
          this->block_data[start_byte_index + block_offset];
    }
    for (u64 block_offset = offset; block_offset < offset + len;
         block_offset++) {
      new_block_data_log[block_offset] = data[block_offset - offset];
    }
    this->now_logging_vec->push_back(
        std::make_shared<BlockOperation>(block_id, new_block_data_log));
    return KNullOk;
  }

  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      return ErrorType::INVALID;
    }
  }

  CHFS_ASSERT((offset + len) <= this->block_size(),
              "Length too big in write_partial_block");
  u64 start_byte_index = block_id * this->block_size();
  for (u64 block_offset = offset; block_offset < offset + len; block_offset++) {
    this->block_data[start_byte_index + block_offset] =
        data[block_offset - offset];
  }

  this->write_fail_cnt++;
  return KNullOk;
}

auto BlockManager::write_partial_block_wo_failure(block_id_t block_id,
                                                  const u8 *data, usize offset,
                                                  usize len) -> ChfsNullResult {
  CHFS_ASSERT((offset + len) <= this->block_size(),
              "Length too big in write_partial_block");
  u64 start_byte_index = block_id * this->block_size();
  for (u64 block_offset = offset; block_offset < offset + len; block_offset++) {
    this->block_data[start_byte_index + block_offset] =
        data[block_offset - offset];
  }
  return KNullOk;
}

auto BlockManager::read_block(block_id_t block_id, u8 *data) -> ChfsNullResult {
  if (this->now_logging_vec) {
    for (auto iterator = now_logging_vec->rbegin();
         iterator != now_logging_vec->rend(); iterator++) {
      if ((*iterator)->block_id_ == block_id) {
        memcpy(data, (*iterator)->new_block_state_.data(), this->block_size());
        return KNullOk;
      }
    }
  }
  u64 start_byte_index = block_id * this->block_size();
  for (u64 block_offset = 0; block_offset < this->block_size();
       block_offset++) {
    data[block_offset] = this->block_data[start_byte_index + block_offset];
  }
  return KNullOk;
}

auto BlockManager::zero_block(block_id_t block_id) -> ChfsNullResult {
  if (this->now_logging_vec) {
    this->now_logging_vec->push_back(std::make_shared<BlockOperation>(
        block_id, std::vector<u8>(this->block_size())));
    return KNullOk;
  }
  u64 start_byte_index = block_id * this->block_size();
  for (u64 block_offset = 0; block_offset < this->block_size();
       block_offset++) {
    this->block_data[start_byte_index + block_offset] = 0;
  }

  return KNullOk;
}

auto BlockManager::sync(block_id_t block_id) -> ChfsNullResult {
  if ((!log_enabled && block_id >= this->block_cnt) ||
      (log_enabled &&
       block_id >= this->block_cnt + BLOCKS_RESERVED_FOR_LOGGING)) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  auto res = msync(this->block_data + block_id * this->block_sz, this->block_sz,
                   MS_SYNC | MS_INVALIDATE);
  if (res != 0) return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

auto BlockManager::flush() -> ChfsNullResult {
  int res;
  if (!log_enabled) {
    res = msync(this->block_data, this->block_sz * this->block_cnt,
                MS_SYNC | MS_INVALIDATE);
  } else {
    res =
        msync(this->block_data,
              this->block_sz * (this->block_cnt + BLOCKS_RESERVED_FOR_LOGGING),
              MS_SYNC | MS_INVALIDATE);
  }
  if (res != 0) return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

BlockManager::~BlockManager() {
  if (!this->in_memory) {
    munmap(this->block_data, this->total_storage_sz());
    close(this->fd);
  } else {
    delete[] this->block_data;
  }
}

// BlockIterator
auto BlockIterator::create(BlockManager *bm, block_id_t start_block_id,
                           block_id_t end_block_id)
    -> ChfsResult<BlockIterator> {
  BlockIterator iter;
  iter.bm = bm;
  iter.cur_block_off = 0;
  iter.start_block_id = start_block_id;
  iter.end_block_id = end_block_id;

  std::vector<u8> buffer(bm->block_sz);

  auto res = bm->read_block(iter.cur_block_off / bm->block_sz + start_block_id,
                            buffer.data());
  if (res.is_ok()) {
    iter.buffer = std::move(buffer);
    return ChfsResult<BlockIterator>(iter);
  }
  return ChfsResult<BlockIterator>(res.unwrap_error());
}

// assumption: a previous call of has_next() returns true
auto BlockIterator::next(usize offset) -> ChfsNullResult {
  auto prev_block_id = this->cur_block_off / bm->block_size();
  this->cur_block_off += offset;

  auto new_block_id = this->cur_block_off / bm->block_size();
  // move forward
  if (new_block_id != prev_block_id) {
    if (this->start_block_id + new_block_id > this->end_block_id) {
      return ChfsNullResult(ErrorType::DONE);
    }

    // else: we need to refresh the buffer
    auto res = bm->read_block(this->start_block_id + new_block_id,
                              this->buffer.data());
    if (res.is_err()) {
      return ChfsNullResult(res.unwrap_error());
    }
  }
  return KNullOk;
}

// log related functions
auto BlockManager::read_bytes_from_log_area(usize start_byte_idx, usize length)
    -> std::vector<u8> {
  std::vector<u8> result;
  result.reserve(length);
  const auto block_siz = this->block_size();
  auto current_log_block_id = start_byte_idx / block_siz + this->block_cnt;
  usize bytes_already_read = 0;
  if (start_byte_idx % block_siz) {
    auto current_offset = start_byte_idx % block_siz;
    auto current_read_length = std::min(length, block_siz - current_offset);
    auto buffer = std::vector<u8>(block_siz);
    auto read_res = read_block(current_log_block_id, buffer.data());
    if (read_res.is_err()) return {};
    result.insert(result.end(), buffer.begin() + current_offset,
                  buffer.begin() + current_offset + current_read_length);
    bytes_already_read += current_read_length;
    current_log_block_id++;
  }
  while (bytes_already_read < length) {
    auto current_read_length = std::min(length - bytes_already_read, block_siz);
    auto buffer = std::vector<u8>(block_siz);
    auto read_res = read_block(current_log_block_id, buffer.data());
    if (read_res.is_err()) return {};
    result.insert(result.end(), buffer.begin(),
                  buffer.begin() + current_read_length);
    bytes_already_read += current_read_length;
    current_log_block_id++;
  }
  return result;
}

auto BlockManager::write_bytes_to_log_area(usize start_byte_idx,
                                           std::vector<u8> data) -> bool {
  const auto block_siz = this->block_size();
  auto current_log_block_id = start_byte_idx / block_siz + this->block_cnt;
  usize length = data.size();
  usize bytes_written = 0;
  if (start_byte_idx % block_siz) {
    auto current_offset = start_byte_idx % block_siz;
    auto current_write_length = std::min(length, block_siz - current_offset);
    auto buffer =
        std::vector<u8>(data.begin(), data.begin() + current_write_length);
    auto write_res =
        write_partial_block_wo_failure(current_log_block_id, buffer.data(),
                                       current_offset, current_write_length);
    sync(current_log_block_id);
    if (write_res.is_err()) return false;
    bytes_written += current_write_length;
    current_log_block_id++;
  }
  while (bytes_written < length) {
    auto current_write_length = std::min(length - bytes_written, block_siz);
    auto buffer =
        std::vector<u8>(data.begin() + bytes_written,
                        data.begin() + bytes_written + current_write_length);
    auto write_res = write_partial_block_wo_failure(
        current_log_block_id, buffer.data(), 0, current_write_length);
    sync(current_log_block_id);
    if (write_res.is_err()) return false;
    bytes_written += current_write_length;
    current_log_block_id++;
  }
  return true;
}

auto BlockManager::get_log_area_start_end_byte_idx()
    -> std::pair<usize, usize> {
  usize buffer_siz = 2 * sizeof(usize);
  auto read_res = read_bytes_from_log_area(0, buffer_siz);
  if (read_res.size() != buffer_siz) return {};
  auto res_p = reinterpret_cast<usize *>(read_res.data());
  return std::make_pair(res_p[0], res_p[1]);
}

auto BlockManager::set_log_area_start_end_byte_idx(usize start, usize end)
    -> bool {
  usize buffer_siz = sizeof(usize);
  auto buffer = std::vector<u8>(buffer_siz);
  auto buffer_p = reinterpret_cast<usize *>(buffer.data());
  if (start) {
    *buffer_p = start;
    auto write_res = write_bytes_to_log_area(0, buffer);
    if (!write_res) return false;
  }
  if (end) {
    *buffer_p = end;
    auto write_res = write_bytes_to_log_area(sizeof(usize), buffer);
    if (!write_res) return false;
  }
  return true;
}

auto BlockManager::write_log_entry(std::vector<u8> entry_vec) -> bool {
  // TODO: add circle, now just stops working when reaches end
  auto entry_p = reinterpret_cast<LogEntry *>(entry_vec.data());
  auto len = entry_p->tell_size();
  if (len != entry_vec.size()) return false;
  const auto &[start_byte, end_byte] = get_log_area_start_end_byte_idx();
  auto write_res = write_bytes_to_log_area(end_byte - 1, entry_vec);
  if (!write_res) return false;
  set_log_area_start_end_byte_idx(0, end_byte + len);
  return true;
}

auto BlockManager::get_log_entries() -> std::vector<PackedLogEntry> {
  // TODO: add circle, now just stops working when reaches end
  const auto &[start_byte, end_byte] = get_log_area_start_end_byte_idx();
  auto current_byte_idx = start_byte;
  auto result = std::vector<PackedLogEntry>();
  if (start_byte > end_byte) {
    return {};  // TODO: read the circular log entries
  }
  while (current_byte_idx < end_byte - 1) {
    auto entry_vec =
        read_bytes_from_log_area(current_byte_idx, sizeof(LogEntry));
    if (entry_vec.size() != sizeof(LogEntry)) return {};
    auto entry_ptr = reinterpret_cast<LogEntry *>(entry_vec.data());
    auto data_vec = std::vector<u8>();
    if (entry_ptr->type == LogEntryType::BLOCK_CHANGE) {
      data_vec = read_bytes_from_log_area(current_byte_idx + sizeof(LogEntry),
                                          DiskBlockSize);
      if (data_vec.size() != DiskBlockSize) return {};
    }
    if (entry_ptr->type != LogEntryType::INVALID &&
        entry_ptr->type != LogEntryType::INVALID_BLOCK_CHANGE) {
      PackedLogEntry packed_entry(*entry_ptr, data_vec);
      result.push_back(packed_entry);
    }
    auto entry_len = entry_ptr->tell_size();
    current_byte_idx += entry_len;
  }
  return result;
}

auto BlockManager::start_logging(
    std::vector<std::shared_ptr<BlockOperation>> *log_vec) -> ChfsNullResult {
  if (log_enabled && log_vec != nullptr) {
    this->now_logging_vec = log_vec;
    return KNullOk;
  }
  return ChfsNullResult(ErrorType::INVALID_ARG);
}

auto BlockManager::stop_logging() -> ChfsNullResult {
  if (this->now_logging_vec == nullptr)
    return ChfsNullResult(ErrorType::INVALID_ARG);
  this->now_logging_vec = nullptr;
  return KNullOk;
}

auto BlockManager::flush_ops(
    std::vector<std::shared_ptr<BlockOperation>> *log_vec) -> ChfsNullResult {
  for (const auto &item : *log_vec) {
    auto write_res =
        this->write_block(item->block_id_, item->new_block_state_.data());
    if (write_res.is_err()) return ChfsNullResult(write_res.unwrap_error());
  }
  return this->flush();
}

void BlockManager::set_max_txn_id(txn_id_t max_txn_id) {
  std::lock_guard txn_id_lock(this->txn_id_mutex);
  std::vector<u8> buffer(sizeof(txn_id_t));
  memcpy(buffer.data(), reinterpret_cast<u8 *>(&max_txn_id), sizeof(txn_id_t));
  this->write_bytes_to_log_area(OFFSET_OF_MAX_TXN_ID, buffer);
}

auto BlockManager::increase_and_get_txn_id() -> txn_id_t {
  std::lock_guard txn_id_lock(this->txn_id_mutex);
  auto buffer =
      this->read_bytes_from_log_area(OFFSET_OF_MAX_TXN_ID, sizeof(txn_id_t));
  if (buffer.size() != sizeof(txn_id_t)) return 0;
  auto id_p = reinterpret_cast<txn_id_t *>(buffer.data());
  auto result = ++(*id_p);
  this->write_bytes_to_log_area(OFFSET_OF_MAX_TXN_ID, buffer);
  return result;
}

void BlockManager::reset_logging_area() {
  set_log_area_start_end_byte_idx(OFFSET_OF_LOGGING_AREA,
                                  OFFSET_OF_LOGGING_AREA + 1);
}

}  // namespace chfs
