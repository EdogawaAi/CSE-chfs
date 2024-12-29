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
  auto num_of_version_block = (KDefaultBlockCnt * sizeof(version_t)) / bm->block_size();
  if (is_initialized) {
    block_allocator_ = std::make_shared<BlockAllocator>(bm, num_of_version_block, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(new BlockAllocator(bm, num_of_version_block, true));
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
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  std::vector<u8> resultBuffer = {};
  if (block_id >= block_allocator_->bm->total_blocks() || (offset + len > BLOCK_SIZE))
  {
    return resultBuffer;
  }
  std::vector<u8> entireBlockBuffer(BLOCK_SIZE);
  auto readResult = block_allocator_->bm->read_block(block_id, entireBlockBuffer.data());
  if(readResult.is_err()){
    return resultBuffer;
  }

  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto versionBlockId = block_id / KVersionPerBlock;
  auto versionInBlockIndex = block_id % KVersionPerBlock;
  std::vector<u8> versionBuffer(BLOCK_SIZE);
  auto read_version_block_res = block_allocator_->bm->read_block(versionBlockId, versionBuffer.data());
  if (read_version_block_res.is_err())
  {
    return resultBuffer;
  }
  auto versionArray = reinterpret_cast<version_t *>(versionBuffer.data());
  auto localVersion = versionArray[versionInBlockIndex];
  if (localVersion != version)
  {
    return resultBuffer;
  }

  resultBuffer.resize(len);
  for (usize i = 0; i < len; ++i)
  {
    resultBuffer[i] = entireBlockBuffer[offset + i];
  }
  return resultBuffer;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  usize len = buffer.size();
  if (block_id >= block_allocator_->bm->total_blocks() || (offset + len > BLOCK_SIZE))
  {
    return false;
  }
  // TODO maybe: check the block is valid
  auto writeResult = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, len);
  if (writeResult.is_err())
  {
    return false;
  }
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  auto allocateResult = block_allocator_->allocate();
  if (allocateResult.is_err())
  {
    //return std::pair<block_id_t, version_t>(0,0);
    return {0, 0};
  }

  block_id_t blockIdResult = allocateResult.unwrap();
  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto versionBlockId = blockIdResult / KVersionPerBlock;
  auto versionInBlockIndex = blockIdResult % KVersionPerBlock;
  std::vector<u8> versionBuffer(BLOCK_SIZE);
  auto readVersionBlockResult = block_allocator_->bm->read_block(versionBlockId, versionBuffer.data());
  if (readVersionBlockResult.is_err())
  {
    return std::pair<block_id_t, version_t>(0,0);
  }
  auto versionArray = reinterpret_cast<version_t *>(versionBuffer.data());
  auto newVersion = versionArray[versionInBlockIndex] + 1;
  versionArray[versionInBlockIndex] = newVersion;
  auto writeVersionBlockResult = block_allocator_->bm->write_block(versionBlockId, versionBuffer.data());
  if (writeVersionBlockResult.is_err())
  {
    return {blockIdResult, newVersion - 1};
  }

  return {blockIdResult, newVersion};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  auto deallocateResult = block_allocator_->deallocate(block_id);
  if (deallocateResult.is_err())
  {
    return false;
  }

  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto versionBlockId = block_id / KVersionPerBlock;
  auto versionInBlockIndex = block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto readResult = block_allocator_->bm->read_block(versionBlockId, version_buf.data());
  if (readResult.is_err())
  {
    return false;
  }
  auto versionArray = reinterpret_cast<version_t *>(version_buf.data());
  auto newVersion = versionArray[versionInBlockIndex] + 1;
  versionArray[versionInBlockIndex] = newVersion;
  auto writeVersionBlockResult = block_allocator_->bm->write_block(versionBlockId, version_buf.data());
  if (writeVersionBlockResult.is_err())
  {
    return false;
  }
  return true;
}
} // namespace chfs