#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto block_size = operation_->block_manager_->block_size();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  std::vector<u8> inode_vector(block_size);
  auto read_inode_res = operation_->inode_manager_->read_inode(parent, inode_vector);
  if (read_inode_res.is_err()) {
    return 0;
  }

  Inode *inode_ptr = reinterpret_cast<Inode *>(inode_vector.data());
  if (inode_ptr->get_type() != InodeType::Directory) {
    return 0;
  }

  std::lock_guard table_lock(inode_table_lock);
  auto result = operation_->mk_helper(parent, name.c_str(),static_cast<InodeType>(type));
  if (result.is_ok())
  {
    return result.unwrap();
  }

  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  std::lock_guard table_lock(inode_table_lock);
  auto result = operation_->unlink(parent, name.c_str());
  if (result.is_ok())
  {
    return true;
  }

  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  auto result = operation_->lookup(parent, name.c_str());
  if (result.is_ok())
  {
    return result.unwrap();
  }

  return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto block_size = operation_->block_manager_->block_size();

  std::vector<u8> block_info(operation_->block_manager_->block_size());
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  auto resd_inode_result = operation_->inode_manager_->read_inode(id, block_info); //对 std::vector<u8> 类型的非 const 左值引用不能绑定到类型 BlockInfo* 的右值
  if (resd_inode_result.is_err())
  {
    return {};
  }

  Inode *inode_ptr = reinterpret_cast<Inode *>(block_info.data());
  auto file_size = inode_ptr->get_size();
  auto block_num = (file_size % block_size == 0) ? (file_size / block_size) : (file_size / block_size + 1);
  std::vector<BlockInfo> result;
  result.reserve(block_num);
  auto *block_info_ptr = reinterpret_cast<BlockInfo *>(inode_ptr->blocks);

  for (int i = 0; i < block_num; i++)
  {
    result.push_back(block_info_ptr[i]);
  }

  return result;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto block_size = operation_->block_manager_->block_size();
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  std::vector<u8> inode_vector(block_size);
  auto read_inode_result = operation_->inode_manager_->read_inode(id, inode_vector);
  if (read_inode_result.is_err())
  {
    return {};
  }

  auto inode_block_id = read_inode_result.unwrap();
  Inode *inode_ptr = reinterpret_cast<Inode *>(inode_vector.data());
  auto block_info_ptr = reinterpret_cast<BlockInfo *>(inode_ptr->blocks);

  //allocate block
  if (inode_ptr->get_type() != InodeType::FILE)
  {
    return {};
  }
  auto file_size = inode_ptr->get_size();
  auto block_num = (file_size % block_size == 0) ? (file_size / block_size) : (file_size / block_size + 1);

  auto client_index = generator.rand(1, num_data_servers);
  auto result = clients_[client_index]->call("alloc_block");

  auto rpc_result = result.unwrap();
  auto rpc_return = rpc_result->as<std::pair<block_id_t, version_t>>();

  inode_ptr->inner_attr.size += block_size;
  inode_ptr->inner_attr.ctime = inode_ptr->inner_attr.mtime = time(nullptr);

  auto new_block_info = std::make_tuple(rpc_return.first, client_index, rpc_return.second);
  block_info_ptr[block_num] = new_block_info;

  auto write_inode_result = operation_->block_manager_->write_block(inode_block_id, inode_vector.data());
  if (write_inode_result.is_err())
  {
    return {};
  }

  return new_block_info;
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // read the inode and check if it is a file
  const auto block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode_vec(block_size);
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(id, inode_vec);
  if (read_inode_res.is_err()) return false;
  auto inode_block_id = read_inode_res.unwrap();
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::FILE) return false;

  // find the block to free
  BlockInfo *block_info_p = reinterpret_cast<BlockInfo *>(inode_p->blocks);
  u32 block_idx_to_remove;
  bool found = false;
  auto file_size = inode_p->get_size();
  auto block_num = file_size % block_size == 0 ? file_size / block_size
                                               : file_size / block_size + 1;
  for (u32 i = 0; i < block_num; i++) {
    if (std::get<0>(block_info_p[i]) == block_id &&
        std::get<1>(block_info_p[i]) == machine_id) {
      block_idx_to_remove = i;
      found = true;
      break;
    }
  }

  // call the data server to free the block
  if (!found) return false;
  auto rpc_res = this->clients_[machine_id]->call("free_block", block_id);
  if (rpc_res.is_err()) return false;
  auto rpc_ret = rpc_res.unwrap()->as<bool>();
  if (!rpc_ret) return false;

  // remove the block from the inode and update attributes
  for (u32 i = block_idx_to_remove; i < block_num - 1; i++) {
    block_info_p[i] = block_info_p[i + 1];
  }
  u64 new_file_size = file_size - block_size;
  inode_p->inner_attr.size = new_file_size;
  inode_p->inner_attr.ctime = inode_p->inner_attr.mtime = time(0);

  // write back the inode
  auto write_inode_res = this->operation_->block_manager_->write_block(
      inode_block_id, inode_vec.data());
  if (write_inode_res.is_err()) return false;
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard inode_lock(inode_locks[node % LOCK_CNT]);
  std::vector<std::pair<std::string, inode_id_t>> result;
  std::list<DirectoryEntry> list;
  read_directory(operation_.get(), node, list);

  for (auto iter = list.begin(); iter != list.end(); iter++)
  {
    result.push_back(std::make_pair(iter->name, iter->id));
  }

  return result;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  auto get_res = operation_->get_type_attr(id);
  if (get_res.is_err())
  {
    return std::tuple<u64, u64, u64, u64, u8>(0, 0, 0, 0, 0);
  }

  auto pair = get_res.unwrap();
  InodeType inode_type = pair.first;
  FileAttr file_attr = pair.second;

  return std::tuple<u64, u64, u64, u64, u8>(file_attr.size, file_attr.atime, file_attr.mtime, file_attr.ctime, static_cast<u8>(inode_type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs