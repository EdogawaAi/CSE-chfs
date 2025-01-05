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
  mutex.lock();

  if (!is_log_enabled_)
  {
    if (type == DirectoryType)
    {
      auto mkdir_res = operation_->mkdir(parent, name.data());
      if (mkdir_res.is_err())
      {
        mutex.unlock();
        return KInvalidInodeID;
      }
      mutex.unlock();
      return mkdir_res.unwrap();
    } else if (type == RegularFileType)
    {
      auto mkfile_res = operation_->mkfile(parent, name.data());
      if (mkfile_res.is_err())
      {
        mutex.unlock();
        return KInvalidInodeID;
      }
      mutex.unlock();
      return mkfile_res.unwrap();
    } else
    {
      mutex.unlock();
      return KInvalidInodeID;
    }
  }
  else
  {
    auto txn_id = commit_log->generate_txn_id();
    std::vector<std::shared_ptr<BlockOperation>> tx_ops;
    if (type == DirectoryType) {
      auto mknode_res = operation_->mknode_atomic(parent, name.data(), InodeType::Directory, tx_ops);
      if (mknode_res.is_err()) {
        mutex.unlock();
        return KInvalidInodeID;
      }
      commit_log->append_log(txn_id, tx_ops);
      for (auto op : tx_ops) {
        auto op_write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
        if (op_write_res.is_err()) {
          mutex.unlock();
          return KInvalidInodeID;
        }
      }
      mutex.unlock();
      return mknode_res.unwrap();
    } else if (type == RegularFileType) {
      auto mknode_res = operation_->mknode_atomic(parent, name.data(), InodeType::FILE, tx_ops);
      if (mknode_res.is_err()) {
        mutex.unlock();
        return KInvalidInodeID;
      }
      commit_log->append_log(txn_id, tx_ops);
      for (auto op : tx_ops) {
        auto op_write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
        if (op_write_res.is_err()) {
          mutex.unlock();
          return KInvalidInodeID;
        }
      }
      mutex.unlock();
      return mknode_res.unwrap();
    } else {
      mutex.unlock();
      return KInvalidInodeID;
    }
  }

  mutex.unlock();
  return KInvalidInodeID;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  mutex.lock();

  if (!is_log_enabled_)
  {
    auto lookup_res = operation_->lookup(parent, name.data());
    if (lookup_res.is_err())
    {
      mutex.unlock();
      return false;
    }

    auto unlink_inode_id = lookup_res.unwrap();
    auto type_res = operation_->gettype(unlink_inode_id);
    if (type_res.is_err())
    {
      mutex.unlock();
      return false;
    }

    auto unlink_type = type_res.unwrap();

    if (unlink_type != InodeType::FILE)
    {
      if (unlink_type == InodeType::Directory)
      {
        auto unlink_res = operation_->unlink(parent, name.data());
        if (unlink_res.is_err())
        {
          mutex.unlock();
          return false;
        }
        mutex.unlock();
        return true;
      } else
      {
        mutex.unlock();
        return false;
      }
    }


    auto block_info_list = get_block_map(unlink_inode_id);
    auto inode_block_res = operation_->inode_manager_->get(unlink_inode_id);
    if (inode_block_res.is_err())
    {
      mutex.unlock();
      return false;
    }

    auto inode_block_id = inode_block_res.unwrap();
    auto free_inode_res = operation_->inode_manager_->free_inode(unlink_inode_id);
    if (free_inode_res.is_err())
    {
      mutex.unlock();
      return false;
    }

    auto local_free_res = operation_->block_allocator_->deallocate(inode_block_id);
    if (local_free_res.is_err())
    {
      mutex.unlock();
      return false;
    }


    for (auto block_info : block_info_list)
    {
      auto [block_id, mac_id, _] = block_info;
      if (clients_.find(mac_id) == clients_.end())
      {
        mutex.unlock();
        return false;
      } else
      {
        auto target_mac = clients_[mac_id];
        auto response = target_mac->call("free_block", block_id);
        if (response.is_err())
        {
          mutex.unlock();
          return false;
        }
        bool is_success = response.unwrap()->as<bool>();
        if (!is_success)
        {
          mutex.unlock();
          return false;
        }
      }
    }

    auto read_parent_res = operation_->read_file(parent);
    if (read_parent_res.is_err())
    {
      mutex.unlock();
      return false;
    }
    auto parent_content = read_parent_res.unwrap();
    std::string parent_content_str(reinterpret_cast<char *>(parent_content.data()), parent_content.size());
    std::string new_parent_content = rm_from_directory(parent_content_str, name);
    std::vector<u8> new_dir(new_parent_content.begin(), new_parent_content.end());
    auto write_res = operation_->write_file(parent, new_dir);
    if (write_res.is_err())
    {
      mutex.unlock();
      return false;
    }

    mutex.unlock();
    return true;
  }
  else
  {
    auto txn_id = commit_log->generate_txn_id();
    std::vector<std::shared_ptr<BlockOperation>> tx_ops;
    auto lookup_res = operation_->lookup_from_memory(parent, name.data(), tx_ops);
    if (lookup_res.is_err()) {
      mutex.unlock();
      return false;
    }

    auto unlink_inode_id = lookup_res.unwrap();
    auto type_res = operation_->gettype_from_memory(unlink_inode_id, tx_ops);
    if (type_res.is_err()) {
      mutex.unlock();
      return false;
    }

    auto unlink_type = type_res.unwrap();
    if (unlink_type != InodeType::FILE) {
      if (unlink_type == InodeType::Directory) {
        auto unlink_res = operation_->unlink_atomic(parent, name.data(), tx_ops);
        if (unlink_res.is_err()) {
          mutex.unlock();
          return false;
        }
        commit_log->append_log(txn_id, tx_ops);
        for (auto op : tx_ops) {
          auto op_write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
          if (op_write_res.is_err()) {
            mutex.unlock();
            return false;
          }
        }
        mutex.unlock();
        return true;
      } else {
        mutex.unlock();
        return false;
      }
    }

    auto block_info_list = this->get_block_map_from_memory(unlink_inode_id, tx_ops);
    auto inode_block_res = operation_->inode_manager_->get_from_memory(unlink_inode_id, tx_ops);
    if (inode_block_res.is_err()) {
      mutex.unlock();
      return false;
    }

    auto inode_block_id = inode_block_res.unwrap();
    auto free_inode_res = operation_->inode_manager_->free_inode_atomic(unlink_inode_id, tx_ops);
    if (free_inode_res.is_err()) {
      mutex.unlock();
      return false;
    }

    auto local_free_res = operation_->block_allocator_->deallocate_atomic(inode_block_id, tx_ops);
    if (local_free_res.is_err()) {
      mutex.unlock();
      return false;
    }

    for (auto block_info : block_info_list) {
      block_id_t block_id = std::get<0>(block_info);
      mac_id_t mac_id = std::get<1>(block_info);
      if (clients_.find(mac_id) == clients_.end()) {
        mutex.unlock();
        return false;
      } else {
        auto target_mac = clients_[mac_id];
        auto response = target_mac->call("free_block", block_id);
        if (response.is_err()) {
          mutex.unlock();
          return false;
        }
        auto is_success = response.unwrap()->as<bool>();
        if (!is_success) {
          mutex.unlock();
          return false;
        }
      }
    }

    auto read_parent_res = operation_->read_file_from_memory(parent, tx_ops);
    if (read_parent_res.is_err()) {
      mutex.unlock();
      return false;
    }


    auto parent_content = read_parent_res.unwrap();
    std::string parent_content_str(reinterpret_cast<char *>(parent_content.data()), parent_content.size());
    std::string parent_content_str_change = rm_from_directory(parent_content_str, name);
    std::vector<u8> new_dir_vec(parent_content_str_change.begin(), parent_content_str_change.end());
    auto write_res = operation_->write_file_atomic(parent, new_dir_vec, tx_ops);
    if (write_res.is_err()) {
      mutex.unlock();
      return false;
    }

    commit_log->append_log(txn_id, tx_ops);
    for (auto op : tx_ops) {
      auto op_write_res = operation_->block_manager_->write_block(op->block_id_, op->new_block_state_.data());
      if (op_write_res.is_err()) {
        mutex.unlock();
        return false;
      }
    }

    mutex.unlock();
    return true;
  }
  mutex.unlock();
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto lookup_res = operation_->lookup(parent, name.data());
  if (lookup_res.is_err())
  {
    return KInvalidInodeID;
  }

  return lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = operation_->block_manager_->block_size();
  std::vector<u8> buffer(BLOCK_SIZE);
  std::vector<BlockInfo> res;

  if (id > operation_->inode_manager_->get_max_inode_supported())
  {
    return res;
  }

  auto type_res = operation_->gettype(id);
  if (type_res.is_err())
  {
    return res;
  }

  auto inode_type = type_res.unwrap();
  if (inode_type != InodeType::FILE)
  {
    return res;
  }

  auto get_res = operation_->inode_manager_->get(id);
  if (get_res.is_err())
  {
    return res;
  }
  auto inode_block_id = get_res.unwrap();
  if (inode_block_id == KInvalidBlockID)
  {
    return res;
  }

  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, buffer.data());
  if (read_block_res.is_err())
  {
    return res;
  }

  auto inode_ptr = reinterpret_cast<Inode *>(buffer.data());

  for (usize i = 0; i < inode_ptr->get_nblocks(); i += 2)
  {
    if (inode_ptr->blocks[i] == KInvalidBlockID)
    {
      break;
    }
    auto block_id = inode_ptr->blocks[i];
    auto mac_id = inode_ptr->blocks[i + 1];
    if (clients_.find(mac_id) == clients_.end())
    {
      res.clear();
      return res;
    }
    auto target_mac = clients_[mac_id];

    auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
    auto version_block_id = block_id / KVersionPerBlock;
    auto version_block_offset = block_id % KVersionPerBlock;

    auto response = target_mac->call("read_data", version_block_id, version_block_offset * sizeof(version_t), sizeof(version_t), 0);
    if (response.is_err())
    {
      res.clear();
      return res;
    }

    auto response_data = response.unwrap()->as<std::vector<u8>>();
    auto version_ptr = reinterpret_cast<version_t *>(response_data.data());
    auto version = *version_ptr;
    res.push_back(BlockInfo(block_id, mac_id, version));
  }

  return res;
}

auto MetadataServer::get_block_map_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> std::vector<BlockInfo> {
  const auto BLOCK_SIZE = operation_->block_manager_->block_size();
  std::vector<u8> buffer(BLOCK_SIZE);
  std::vector<BlockInfo> res(0);

  if (id > operation_->inode_manager_->get_max_inode_supported()) {
    return res;
  }
  auto type_res = operation_->gettype_from_memory(id, tx_ops);
  if (type_res.is_err()) {
    return res;
  }
  auto inode_type = type_res.unwrap();
  if (inode_type != InodeType::FILE) {
    return res;
  }
  auto get_res = operation_->inode_manager_->get_from_memory(id, tx_ops);
  if (get_res.is_err()) {
    return res;
  }
  auto inode_block_id = get_res.unwrap();
  if (inode_block_id == KInvalidBlockID) {
    return res;
  }
  auto read_block_res = operation_->block_manager_->read_block_from_memory(inode_block_id, buffer.data(), tx_ops);
  if (read_block_res.is_err()) {
    return res;
  }
  Inode* inode_ptr = reinterpret_cast<Inode *>(buffer.data());

  for (uint i = 0; i < inode_ptr->get_nblocks(); i += 2) {
    if (inode_ptr->blocks[i] == KInvalidBlockID) {
      break;
    }
    block_id_t block_id = inode_ptr->blocks[i];
    mac_id_t mac_id = static_cast<mac_id_t>(inode_ptr->blocks[i + 1]);

    if (clients_.find(mac_id) == clients_.end()) {
      res.clear();
      return res;
    }
    auto target_mac = clients_[mac_id];
    auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
    auto version_block_id = block_id / KVersionPerBlock;
    auto version_in_block_idx = block_id % KVersionPerBlock;
    auto response = target_mac->call("read_data", version_block_id, version_in_block_idx * sizeof(version_t), sizeof(version_t), 0);
    if (response.is_err()) {
      res.clear();
      return res;
    }
    auto response_vec = response.unwrap()->as<std::vector<u8>>();
    auto version_ptr = reinterpret_cast<version_t *>(response_vec.data());
    version_t version_id = *version_ptr;
    res.push_back(BlockInfo(block_id, mac_id, version_id));
  }
  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  mutex.lock();
  if (id > operation_->inode_manager_->get_max_inode_supported())
  {
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  const auto BLOCK_SIZE = operation_->block_manager_->block_size();
  usize old_block_num = 0;

  std::vector<u8> file_inode(BLOCK_SIZE);
  auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
  auto get_res = operation_->inode_manager_->get(id);
  if (get_res.is_err())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  auto inode_block_id = get_res.unwrap();
  if (inode_block_id == KInvalidBlockID)
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
  if (read_block_res.is_err())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }



  old_block_num = 0;
  for (uint i = 0; i < inode_ptr->get_nblocks(); i += 2)
  {
    if (inode_ptr->blocks[i] == KInvalidBlockID)
    {
      old_block_num = i / 2;
      break;
    }
  }

  if (2 * (old_block_num + 1) > inode_ptr->get_nblocks())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  auto index = 2 * old_block_num;

  std::vector<mac_id_t> mac_ids;
  for (const auto &[mac_id, _] : clients_)
  {
    mac_ids.push_back(mac_id);
  }

  auto rand_num = generator.rand(0, mac_ids.size() - 1);
  auto target_mac_id = mac_ids[rand_num];
  if (clients_.find(target_mac_id) == clients_.end())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  auto target_mac = clients_[target_mac_id];
  auto response = target_mac->call("alloc_block");
  if (response.is_err())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, target_mac_id, 0);
  }

  auto [block_id, version_id] = response.unwrap()->as<std::pair<block_id_t, version_t>>();

  inode_ptr->set_size(inode_ptr->get_size() + BLOCK_SIZE);
  inode_ptr->set_block_direct(index, block_id);
  inode_ptr->set_block_direct(index + 1, target_mac_id);

  auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
  if (write_res.is_err())
  {
    mutex.unlock();
    return BlockInfo(KInvalidBlockID, 0, 0);
  }

  mutex.unlock();
  return BlockInfo(block_id, target_mac_id, version_id);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  mutex.lock();
  if (id > operation_->inode_manager_->get_max_inode_supported())
  {
    mutex.unlock();
    return false;
  }

  if (block_id == KInvalidBlockID)
  {
    mutex.unlock();
    return false;
  }

  const auto BLOCK_SIZE = operation_->block_manager_->block_size();

  std::vector<u8> file_inode(BLOCK_SIZE);
  auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
  auto get_res = operation_->inode_manager_->get(id);
  if (get_res.is_err())
  {
    mutex.unlock();
    return false;
  }

  auto inode_block_id = get_res.unwrap();
  if (inode_block_id == KInvalidBlockID)
  {
    mutex.unlock();
    return false;
  }

  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
  if (read_block_res.is_err())
  {
    mutex.unlock();
    return false;
  }



  bool is_found = false;
  uint record_index = 0;
  for (uint i = 0; i < inode_ptr->get_nblocks(); i += 2)
  {
    if (inode_ptr->blocks[i] == block_id && inode_ptr->blocks[i + 1] == machine_id)
    {
      is_found = true;
      record_index = i;
      break;
    }
  }

  if (!is_found)
  {
    mutex.unlock();
    return false;
  }

  if (clients_.find(machine_id) == clients_.end())
  {
    mutex.unlock();
    return false;
  }

  auto target_mac = clients_[machine_id];
  auto response = target_mac->call("free_block", block_id);
  if (response.is_err())
  {
    mutex.unlock();
    return false;
  }

  bool is_success = response.unwrap()->as<bool>();
  if (!is_success)
  {
    mutex.unlock();
    return false;
  }

  for (uint i = record_index; i < inode_ptr->get_nblocks(); i += 2)
  {
    if (i + 3 >= inode_ptr->get_nblocks())
    {
      inode_ptr->blocks[i] = KInvalidBlockID;
      inode_ptr->blocks[i + 1] = 0;
      break;
    }
    inode_ptr->blocks[i] = inode_ptr->blocks[i + 2];
    inode_ptr->blocks[i + 1] = inode_ptr->blocks[i + 3];
    if (inode_ptr->blocks[i + 2] == KInvalidBlockID)
    {
      break;
    }
  }

  inode_ptr->set_size(inode_ptr->get_size() - BLOCK_SIZE);

  auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
  if (write_res.is_err())
  {
    mutex.unlock();
    return false;
  }

  mutex.unlock();
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::vector<std::pair<std::string, inode_id_t>> res;
  std::list<DirectoryEntry> entries;
  auto read_res = read_directory(operation_.get(), node, entries);
  if (read_res.is_err())
  {
    return res;
  }

  for (const auto &entry : entries)
  {
    auto [name, id] = entry;
    res.push_back(std::make_pair(name, id));
  }

  return res;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  mutex.lock();

  auto get_res = operation_->get_type_attr(id);
  if (get_res.is_err())
  {
    mutex.unlock();
    return std::make_tuple(0, 0, 0, 0, 0);
  }
  auto [inode_type, file_attr] = get_res.unwrap();

  mutex.unlock();
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