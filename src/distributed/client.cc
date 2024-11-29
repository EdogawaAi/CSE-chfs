#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto response = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = response.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidInodeID){
    auto error_code = ErrorType::NotPermitted;
    return ChfsResult<inode_id_t>(error_code);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("unlink", parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto is_success = response.unwrap()->as<bool>();
  if(!is_success){
    auto error_code = ErrorType::NotPermitted;
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto response = metadata_server_->call("lookup", parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = response.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidInodeID){
    auto error_code = ErrorType::INVALID;
    return ChfsResult<inode_id_t>(error_code);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("readdir", id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(error_code);
  }
  auto pair_vec = response.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(pair_vec);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("get_type_attr", id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<std::pair<InodeType, FileAttr>>(error_code);
  }
  auto res = response.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto type = std::get<4>(res);
  InodeType inode_type;
  if(type == DirectoryType){
    inode_type = InodeType::Directory;
  }
  else if(type == RegularFileType){
    inode_type = InodeType::FILE;
  }
  else{
    inode_type = InodeType::Unknown;
  }
  FileAttr file_attr;
  file_attr.size = std::get<0>(res);
  file_attr.atime = std::get<1>(res);
  file_attr.mtime = std::get<2>(res);
  file_attr.ctime = std::get<3>(res);
  std::pair<InodeType, FileAttr> res_pair(inode_type, file_attr);
  return ChfsResult<std::pair<InodeType, FileAttr>>(res_pair);
}

  auto ChfsClient::read_single_block(block_id_t block_id, mac_id_t mac_id, version_t version, usize offset, usize len) -> ChfsResult<std::vector<u8>>
  {
    auto response = data_servers_[mac_id]->call("read_data", block_id, offset, len, version);
  if (response.is_err())
  {
    auto error_code = response.unwrap_error();
    return ChfsResult<std::vector<u8>>(error_code);
  }
  auto data = response.unwrap()->as<std::vector<u8>>();
  if (data.size() != len)
  {
    return ChfsResult<std::vector<u8>>(ErrorType::NotPermitted);
  }
  return ChfsResult<std::vector<u8>>(data);
  }


/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  // first, get the block mapping from metadata server
  // as a client, we don't have any method to know how large a block is in dataserver, so we use default value
  auto block_map_rpc_res = metadata_server_->call("get_block_map", id);
  if (block_map_rpc_res.is_err())
    return ChfsResult<std::vector<u8>>(block_map_rpc_res.unwrap_error());
  auto block_map = block_map_rpc_res.unwrap()->as<std::vector<BlockInfo>>();
  if (block_map.empty()) return ChfsResult<std::vector<u8>>({});
  auto file_length = block_map.size() * BLOCK_SIZE;
  auto file_content = std::vector<u8>(0);
  if (offset + size > file_length)
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  file_content.reserve(size);
  usize size_already_read = 0;
  usize current_block_idx = offset / BLOCK_SIZE;
  if (offset % BLOCK_SIZE != 0) {
    const auto current_read_len = BLOCK_SIZE - offset % BLOCK_SIZE;
    const auto &current_block_info = block_map[current_block_idx];
    const auto &[block_id, mac_id, version] = current_block_info;
    auto read_block_res = read_single_block(
        block_id, mac_id, version, offset % BLOCK_SIZE, current_read_len);
    if (read_block_res.is_err())
      return ChfsResult<std::vector<u8>>(read_block_res.unwrap_error());
    auto buffer = read_block_res.unwrap();
    file_content.insert(file_content.end(), buffer.begin(), buffer.end());
    current_block_idx++;
    size_already_read += current_read_len;
  }
  while (size_already_read < size) {
    const auto current_read_len =
        std::min(BLOCK_SIZE, size - size_already_read);
    const auto &current_block_info = block_map[current_block_idx];
    const auto &[block_id, mac_id, version] = current_block_info;
    auto read_block_res =
        read_single_block(block_id, mac_id, version, 0, current_read_len);
    if (read_block_res.is_err())
      return ChfsResult<std::vector<u8>>(read_block_res.unwrap_error());
    auto buffer = read_block_res.unwrap();
    file_content.insert(file_content.end(), buffer.begin(), buffer.end());
    current_block_idx++;
    size_already_read += current_read_len;
  }

  return ChfsResult<std::vector<u8>>(file_content);
}

auto ChfsClient::write_single_block(block_id_t block_id, mac_id_t mac_id, version_t version, usize offset, std::vector<u8> buffer) -> ChfsNullResult
{
  auto response = data_servers_[mac_id]->call("write_data", block_id, offset, buffer);
  if (response.is_err())
  {
    auto error_code = response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto is_success = response.unwrap()->as<bool>();
  if (!is_success)
  {
    return ChfsNullResult(ErrorType::NotPermitted);
  }
  return KNullOk;
}


// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  auto block_map_rpc_res = metadata_server_->call("get_block_map", id);
  if (block_map_rpc_res.is_err())
    return ChfsNullResult(block_map_rpc_res.unwrap_error());
  auto block_map = block_map_rpc_res.unwrap()->as<std::vector<BlockInfo>>();
  auto old_file_blocks_cnt = block_map.size();
  auto old_file_size = old_file_blocks_cnt * BLOCK_SIZE;
  // make sure the file size is aligned to block size (though it should be)
  auto current_file_size =
      old_file_size + ((old_file_size % BLOCK_SIZE == 0)
                           ? (0ul)
                           : (BLOCK_SIZE - old_file_size % BLOCK_SIZE));
  auto buffer = std::vector<u8>(BLOCK_SIZE);  // inited as zero
  usize write_length = data.size();
  while (current_file_size < offset + write_length) {
    auto allocation_res = metadata_server_->call("alloc_block", id);
    if (allocation_res.is_err())
      return ChfsNullResult(allocation_res.unwrap_error());
    auto block_info = allocation_res.unwrap()->as<BlockInfo>();
    const auto &[block_id, mac_id, version] = block_info;
    auto write_res = write_single_block(block_id, mac_id, version, 0,
                                        buffer);  // zero the allocated block
    if (write_res.is_err()) return ChfsNullResult(write_res.unwrap_error());
    block_map.push_back(block_info);
    current_file_size += BLOCK_SIZE;
  }

  auto current_block_idx = offset / BLOCK_SIZE;
  usize size_written = 0;
  if (offset % BLOCK_SIZE) {
    auto current_write_length = BLOCK_SIZE - offset % BLOCK_SIZE;
    buffer = std::vector<u8>(data.begin(), data.begin() + current_write_length);
    const auto current_block_info = block_map[current_block_idx];
    const auto &[block_id, mac_id, version] = current_block_info;
    auto write_res = write_single_block(block_id, mac_id, version,
                                        offset % BLOCK_SIZE, buffer);
    if (write_res.is_err()) return ChfsNullResult(write_res.unwrap_error());
    size_written += current_write_length;
    current_block_idx++;
  }
  while (size_written < write_length) {
    auto current_write_length =
        std::min(BLOCK_SIZE, write_length - size_written);
    if (write_length - size_written >= BLOCK_SIZE) {
      buffer = std::vector<u8>(data.begin() + size_written,
                               data.begin() + size_written + BLOCK_SIZE);
    } else {
      buffer = std::vector<u8>(data.begin() + size_written, data.end());
    }
    const auto current_block_info = block_map[current_block_idx];
    const auto &[block_id, mac_id, version] = current_block_info;
    auto write_res = write_single_block(block_id, mac_id, version, 0, buffer);
    if (write_res.is_err()) return ChfsNullResult(write_res.unwrap_error());
    size_written += current_write_length;
    current_block_idx++;
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("free_block", id, block_id, mac_id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto is_success = response.unwrap()->as<bool>();
  if(!is_success){
    auto error_code = ErrorType::NotExist;
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

} // namespace chfs