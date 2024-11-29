#include "distributed/dataserver.h"

#include "common/util.h"

namespace chfs {

auto DataServer::get_block_version(block_id_t block_id) -> version_t {
  const auto version_block_id = block_id / versions_per_block;
  const auto block_size = this->block_allocator_->bm->block_size();
  auto buffer = std::vector<u8>(block_size);
  auto read_res =
      this->block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (read_res.is_err()) return 0;
  auto versions = reinterpret_cast<version_t *>(buffer.data());
  return versions[block_id % versions_per_block];
}

auto DataServer::update_block_version(block_id_t block_id) -> version_t {
  const auto version_block_id = block_id / versions_per_block;
  const auto block_size = this->block_allocator_->bm->block_size();
  auto buffer = std::vector<u8>(block_size);
  auto read_res =
      this->block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (read_res.is_err()) return 0;
  auto versions = reinterpret_cast<version_t *>(buffer.data());
  auto new_version = ++versions[block_id % versions_per_block];
  auto write_res =
      this->block_allocator_->bm->write_block(version_block_id, buffer.data());
  if (write_res.is_err()) return 0;
  return new_version;
}

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));

  this->versions_per_block = bm->block_size() / sizeof(version_t);
  this->version_block_num =
      (KDefaultBlockCnt * sizeof(version_t)) / bm->block_size();
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_num, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_block_num, true));
    // zero the version blocks
    for (u32 i = 0; i < version_block_num; i++) {
      bm->zero_block(i);
    }
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  // TODO: Implement version.
  if (this->get_block_version(block_id) != version)
    return {};  // version mismatch
  usize block_siz = this->block_allocator_->bm->block_size();
  if (offset + len > block_siz) return {};
  std::vector<u8> buffer;
  buffer.resize(block_siz);
  this->block_allocator_->bm->read_block(block_id, buffer.data());
  std::vector<u8> result(buffer.begin() + offset,
                         buffer.begin() + len + offset);
  return result;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  usize block_siz = this->block_allocator_->bm->block_size();
  usize len = buffer.size();
  if (offset + len > block_siz) return false;
  auto write_result = this->block_allocator_->bm->write_partial_block(
      block_id, buffer.data(), offset, buffer.size());
  if (write_result.is_err()) return false;
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // TODO: Implement version.
  std::lock_guard lock(this->allocator_lock);
  auto allocate_result = this->block_allocator_->allocate();
  if (allocate_result.is_err()) return {};
  auto block_id = allocate_result.unwrap();
  auto version = update_block_version(block_id);
  return std::make_pair(block_id, version);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  std::lock_guard lock(this->allocator_lock);
  update_block_version(block_id);
  auto free_result = this->block_allocator_->deallocate(block_id);
  if (free_result.is_ok()) {
    return true;
  }
  return false;
}
}  // namespace chfs