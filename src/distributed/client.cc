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
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if (response.is_err()) {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }

  auto inode_id = response.unwrap()->as<inode_id_t>();
  if (inode_id == KInvalidInodeID)
  {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("unlink", parent, name);
  if (response.is_err()) {
    return ChfsNullResult(response.unwrap_error());
  }

  bool is_success = response.unwrap()->as<bool>();
  if (!is_success)
  {
    return ChfsNullResult(ErrorType::INVALID);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("lookup", parent, name);
  if (response.is_err()) {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  auto inode_id = response.unwrap()->as<inode_id_t>();
  if (inode_id == KInvalidInodeID)
  {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("readdir", id);
  if (response.is_err()) {
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(response.unwrap_error());
  }
  auto result = response.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(result);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("get_type_attr", id);
  if (response.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(response.unwrap_error());
  }

  auto res = response.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto [size, atime, mtime, ctime, type] = res;
  InodeType inodeType;
  if (type == DirectoryType)
  {
    inodeType = InodeType::Directory;
  } else if(type == RegularFileType) {
    inodeType = InodeType::FILE;
  } else{
    inodeType = InodeType::Unknown;
  }

  FileAttr fileAttribute;
  fileAttribute.size = size;
  fileAttribute.atime = atime;
  fileAttribute.mtime = mtime;
  fileAttribute.ctime = ctime;
  std::pair<InodeType, FileAttr> resultPair(inodeType, fileAttribute);
  return ChfsResult<std::pair<InodeType, FileAttr>>(resultPair);
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  auto getBlockMapResponse = metadata_server_->call("get_block_map", id);
  if (getBlockMapResponse.is_err())
  {
    return ChfsResult<std::vector<u8>>(getBlockMapResponse.unwrap_error());
  }
  auto blockInfo = getBlockMapResponse.unwrap()->as<std::vector<BlockInfo>>();
  auto fileSize = blockInfo.size() * BLOCK_SIZE;
  if (offset + size > fileSize)
  {
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  }

  std::vector<u8> result(size);
  auto startReadIndex = offset / BLOCK_SIZE;
  auto startReadOffset = offset % BLOCK_SIZE;
  auto endReadIndex = ((offset + size) % BLOCK_SIZE) ? ((offset + size) / BLOCK_SIZE + 1) : ((offset + size) / BLOCK_SIZE);
  auto endReadOffset = ((offset + size) % BLOCK_SIZE) ? ((offset + size) % BLOCK_SIZE) : BLOCK_SIZE;
  usize curOffset = 0;
  for (auto iter = blockInfo.begin() + startReadIndex; iter != blockInfo.begin() + endReadIndex; ++iter)
  {
    auto [blockId, macId, versionId] = *iter;
    if (data_servers_.find(macId) == data_servers_.end())
    {
      return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
    }
    auto targetMac = data_servers_[macId];

    auto readResponse = targetMac->call("read_data", blockId, 0, BLOCK_SIZE, versionId);
    if (readResponse.is_err())
    {
      return ChfsResult<std::vector<u8>>(readResponse.unwrap_error());
    }
    auto readSeq = readResponse.unwrap()->as<std::vector<u8>>();
    if (iter == blockInfo.begin() + startReadIndex && iter == blockInfo.begin() + (endReadIndex - 1))
    {
      std::copy(readSeq.begin() + startReadOffset, readSeq.begin() + endReadOffset, result.begin());
      return ChfsResult<std::vector<u8>>(result);
    }
    if (iter == blockInfo.begin() + startReadIndex)
    {
      std::copy(readSeq.begin() + startReadOffset, readSeq.end(), result.begin());
      curOffset += BLOCK_SIZE - startReadOffset;
    } else if (iter == blockInfo.begin() + (endReadIndex - 1))
    {
      std::copy_n(readSeq.begin(), endReadOffset, result.begin() + curOffset);
      curOffset += endReadOffset;
    } else
    {
      std::copy_n(readSeq.begin(), BLOCK_SIZE, result.begin() + curOffset);
      curOffset += BLOCK_SIZE;
    }
  }
  return ChfsResult<std::vector<u8>>(result);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = DiskBlockSize;
  auto writeLength = data.size();
  auto getBlockMapResponse = metadata_server_->call("get_block_map", id);
  if (getBlockMapResponse.is_err())
  {
    return ChfsNullResult(getBlockMapResponse.unwrap_error());
  }
  auto blockInfo = getBlockMapResponse.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto oldFileSize = blockInfo.size() * BLOCK_SIZE;

  if (offset + writeLength > oldFileSize)
  {
    auto newBlockCnt = ((offset + writeLength) % BLOCK_SIZE) ? ((offset + writeLength) / BLOCK_SIZE + 1) : ((offset + writeLength) / BLOCK_SIZE);
    auto oldBlockCnt = blockInfo.size();

    for (auto i = oldBlockCnt; i < newBlockCnt; ++i)
    {
      auto allocResponse = metadata_server_->call("alloc_block", id);
      if (allocResponse.is_err())
      {
        return ChfsNullResult(allocResponse.unwrap_error());
      }


      auto newBlockInfo = allocResponse.unwrap()->as<BlockInfo>();
      blockInfo.push_back(newBlockInfo);
    }
  }

  auto writeStartIndex = offset / BLOCK_SIZE;
  auto writeStartOffset = offset % BLOCK_SIZE;
  auto writeEndIndex = ((offset + writeLength) % BLOCK_SIZE) ? ((offset + writeLength) / BLOCK_SIZE + 1) : ((offset + writeLength) / BLOCK_SIZE);
  auto writeEndOffset = ((offset + writeLength) % BLOCK_SIZE) ? ((offset + writeLength) % BLOCK_SIZE) : BLOCK_SIZE;


  usize currentOffset = 0;
  for (auto it = blockInfo.begin() + writeStartIndex; it != blockInfo.begin() + writeEndIndex; ++it)
  {
    auto [blockId, macId, _] = *it;
    if (data_servers_.find(macId) == data_servers_.end()){
      return ChfsNullResult(ErrorType::INVALID_ARG);
    }
    auto targetMac = data_servers_[macId];
    std::vector<u8> writeBuffer = std::vector<u8>();
    usize perWriteOffset = 0;
    if (it == blockInfo.begin() + writeStartIndex && it == blockInfo.begin() + (writeEndIndex - 1))
    {
      auto writeResponse = targetMac->call("write_data", blockId, writeStartOffset, data);
      if (writeResponse.is_err())
      {
        return ChfsNullResult(writeResponse.unwrap_error());
      }
      bool isSuccess = writeResponse.unwrap()->as<bool>();
      if (!isSuccess)
      {
        return ChfsNullResult(ErrorType::INVALID);
      }
      return KNullOk;
    }
    if (it == blockInfo.begin() + writeStartIndex)
    {
      writeBuffer.resize(BLOCK_SIZE - writeStartOffset);
      std::copy_n(data.begin(), BLOCK_SIZE - writeStartOffset, writeBuffer.begin());
      perWriteOffset = writeStartOffset;
      currentOffset += BLOCK_SIZE - writeStartOffset;
    } else if (it == blockInfo.begin() + (writeEndIndex - 1))
    {
      writeBuffer.resize(writeEndOffset);
      std::copy_n(data.begin() + currentOffset, writeEndOffset, writeBuffer.begin());
      perWriteOffset = 0;
      currentOffset += writeEndOffset;
    } else
    {
      writeBuffer.resize(BLOCK_SIZE);
      std::copy_n(data.begin() + currentOffset, BLOCK_SIZE, writeBuffer.begin());
      perWriteOffset = 0;
      currentOffset += BLOCK_SIZE;
    }
    auto writeResponse = targetMac->call("write_data", blockId, perWriteOffset, writeBuffer);
    if (writeResponse.is_err()){
      return ChfsNullResult(writeResponse.unwrap_error());
    }
    bool isSuccess = writeResponse.unwrap()->as<bool>();
    if (!isSuccess)
    {
      return ChfsNullResult(ErrorType::INVALID);
    }
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("free_block", id, block_id, mac_id);
  if (response.is_err()) {
    return ChfsNullResult(response.unwrap_error());
  }

  bool isSuccess = response.unwrap()->as<bool>();
  if (!isSuccess)
  {
    return ChfsNullResult(ErrorType::NotExist);
  }
  return KNullOk;
}

} // namespace chfs