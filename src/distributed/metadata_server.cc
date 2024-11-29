#include "distributed/metadata_server.h"

#include <fstream>

#include "common/util.h"
#include "filesystem/directory_op.h"

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
      0;  // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_) operation_->block_manager_->set_may_fail(true);
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
    : is_log_enabled_(is_log_enabled),
      may_failed_(may_failed),
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
    : is_log_enabled_(is_log_enabled),
      may_failed_(may_failed),
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
  // check if the inode is a directory
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(parent, inode_vec);
  if (read_inode_res.is_err()) return {};
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::Directory) return 0;

  // call the mk_helper of FileOperation to do this
  std::lock_guard table_lock(inode_table_lock);

  std::vector<std::shared_ptr<BlockOperation>> op_vec;
  if (is_log_enabled_) {
    this->operation_->block_manager_->start_logging(&op_vec);
  }
  auto result = this->operation_->mk_helper(parent, name.c_str(),
                                            static_cast<chfs::InodeType>(type));
  if (is_log_enabled_) {
    this->operation_->block_manager_->stop_logging();
    auto txn_id = this->operation_->block_manager_->increase_and_get_txn_id();
    this->commit_log->append_log(txn_id, op_vec);
    auto flush_result = this->operation_->block_manager_->flush_ops(&op_vec);
    if (flush_result.is_err()) return 0;
    this->commit_log->commit_log(txn_id);
    this->commit_log->checkpoint();
  }

  if (result.is_ok()) return result.unwrap();
  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // check if the inode is a directory
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(parent, inode_vec);
  if (read_inode_res.is_err()) return {};
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::Directory) return 0;

  std::lock_guard table_lock(inode_table_lock);

  std::vector<std::shared_ptr<BlockOperation>> op_vec;
  if (is_log_enabled_) {
    this->operation_->block_manager_->start_logging(&op_vec);
  }

  auto result = this->operation_->unlink(parent, name.c_str());
  if (is_log_enabled_) {
    this->operation_->block_manager_->stop_logging();
    auto txn_id = this->operation_->block_manager_->increase_and_get_txn_id();
    this->commit_log->append_log(txn_id, op_vec);
    auto flush_result = this->operation_->block_manager_->flush_ops(&op_vec);
    if (flush_result.is_err()) return 0;
    this->commit_log->commit_log(txn_id);
    this->commit_log->checkpoint();
  }

  if (result.is_ok()) return true;
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // check if the inode is a directory
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard parent_lock(inode_locks[parent % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(parent, inode_vec);
  if (read_inode_res.is_err()) return {};
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::Directory) return 0;

  auto result = this->operation_->lookup(parent, name.c_str());
  if (result.is_ok()) {
    return result.unwrap();
  }

  return 0;
}

// utility function to calculate how many blocks are there in the file
auto calculate_block_cnt(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  const auto block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode_vec(block_size);
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(id, inode_vec);
  if (read_inode_res.is_err()) {
    return {};
  }
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::FILE)
    return {};  // should not read a directory as a file

  auto file_size = inode_p->get_size();
  auto block_num = calculate_block_cnt(file_size, block_size);
  std::vector<BlockInfo> result = std::vector<BlockInfo>();
  result.reserve(block_num);
  auto *block_info_p = reinterpret_cast<BlockInfo *>(inode_p->blocks);
  for (chfs::u32 i = 0; i < block_num; i++) {
    result.push_back(block_info_p[i]);
  }

  return result;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // read the inode
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(id, inode_vec);
  if (read_inode_res.is_err()) return {};
  auto inode_block_id = read_inode_res.unwrap();
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  auto block_info_p = reinterpret_cast<BlockInfo *>(inode_p->blocks);

  // allocate the block
  if (inode_p->get_type() != InodeType::FILE)
    return {};  // should not allocate block like this for a directory
  auto file_size = inode_p->get_size();
  auto block_num = calculate_block_cnt(file_size, block_size);
  if (block_num + 1 > inode_p->get_nblocks_metadata_server())
    return {};  // too many blocks in the inode
  auto client_idx = this->generator.rand(1, this->num_data_servers);
  auto result = this->clients_[client_idx]->call("alloc_block");
  if (result.is_err()) return {};
  auto rpc_res = result.unwrap();
  auto rpc_ret = rpc_res->as<std::pair<block_id_t, version_t>>();
  if (rpc_ret == std::pair<block_id_t, version_t>()) return {};

  // update the inode
  inode_p->inner_attr.size += block_size;
  inode_p->inner_attr.ctime = inode_p->inner_attr.mtime = time(0);
  auto new_block_info =
      std::make_tuple(rpc_ret.first, client_idx, rpc_ret.second);
  block_info_p[block_num] = new_block_info;

  // write back the inode
  auto write_inode_res = this->operation_->block_manager_->write_block(
      inode_block_id, inode_vec.data());
  if (write_inode_res.is_err()) return {};

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
  chfs::u32 block_idx_to_remove;
  bool found = false;
  auto file_size = inode_p->get_size();
  auto block_num = calculate_block_cnt(file_size, block_size);
  for (chfs::u32 i = 0; i < block_num; i++) {
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
  for (chfs::u32 i = block_idx_to_remove; i < block_num - 1; i++) {
    block_info_p[i] = block_info_p[i + 1];
  }
  // chfs::u64 new_file_size = ((block_idx_to_remove == block_num - 1ul)
  //                                ? (file_size % block_size == 0
  //                                       ? (file_size - block_size)
  //                                       : (file_size - file_size %
  //                                       block_size))
  //                                : (file_size - block_size));
  chfs::u64 new_file_size = file_size - block_size;
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
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard inode_lock(inode_locks[node % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(node, inode_vec);
  if (read_inode_res.is_err()) return {};
  Inode *inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  if (inode_p->get_type() != InodeType::Directory)
    return {};  // should not read a file as a directory

  std::list<DirectoryEntry> dir_list;
  auto read_dir_res = read_directory(this->operation_.get(), node, dir_list);
  if (read_dir_res.is_err()) return {};
  std::vector<std::pair<std::string, inode_id_t>> dir_vec;
  for (const auto &item : dir_list) {
    dir_vec.push_back(std::make_pair(item.name, item.id));
  }
  return dir_vec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  const auto block_size = this->operation_->block_manager_->block_size();
  std::lock_guard inode_lock(inode_locks[id % LOCK_CNT]);
  std::vector<u8> inode_vec(block_size);
  auto read_inode_res =
      this->operation_->inode_manager_->read_inode(id, inode_vec);
  if (read_inode_res.is_err()) return {};
  auto inode_p = reinterpret_cast<Inode *>(inode_vec.data());
  const auto &attr = inode_p->inner_attr;
  return std::make_tuple(attr.size, attr.atime, attr.mtime, attr.ctime,
                         static_cast<u8>(inode_p->get_type()));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running) return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

}  // namespace chfs