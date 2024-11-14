#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

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
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
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
  // UNIMPLEMENTED();
 if (get_block_version(block_id) != version)
 {
   return {};
 }
  auto block_size = block_allocator_->bm->block_size();
  if (offset + len > block_size)
  {
    // throw std::runtime_error("Read out of block boundary");
    return {};
  }

  std::vector<u8> buffer(block_size);
  block_allocator_->bm->read_block(block_id, buffer.data());
  std::vector<u8> result(buffer.begin() + offset, buffer.begin() + offset + len);

  return result;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::vector<u8> block_data (block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(block_id, block_data.data());

  if (offset + buffer.size() > block_data.size())
  {
    // throw std::runtime_error("Write out of block boundary");
    return false;
  }

  std::copy(buffer.begin(), buffer.end(), block_data.begin() + offset);

  block_allocator_->bm->write_block(block_id, block_data.data());

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
 auto allocate_result = block_allocator_->allocate();
  if (allocate_result.is_err())
  {
    return {0, 0};
  }

  block_id_t block_id = allocate_result.unwrap();
  auto new_version = update_block_version(block_id);
  return {block_id, new_version};

}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  if (block_allocator_->deallocate(block_id).is_err())
  {
    return false;
  }
  update_block_version(block_id);

  return true;
}
  auto DataServer::get_block_version(block_id_t block_id) -> version_t
  {
    auto version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
    block_id_t block_idx = block_id / version_per_block;
    block_id_t version_idx = block_id % version_per_block;

    std::vector<u8> version_block(block_allocator_->bm->block_size());
    block_allocator_->bm->read_block(block_idx, version_block.data());

    auto version_ptr = reinterpret_cast<version_t *>(version_block.data());
    return version_ptr[version_idx];
  }

  auto DataServer::update_block_version(block_id_t block_id) -> version_t
  {
    auto version_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
  block_id_t block_idx = block_id / version_per_block;
  block_id_t version_idx = block_id % version_per_block;

  std::vector<u8> version_block(block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(block_idx, version_block.data());

  auto version_ptr = reinterpret_cast<version_t *>(version_block.data());
  version_ptr[version_idx]++;

  block_allocator_->bm->write_block(block_idx, version_block.data());
  return version_ptr[version_idx];
  }


} // namespace chfs