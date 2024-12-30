#include "filesystem/operations.h"
#include "metadata/superblock.h"

namespace chfs {

FileOperation::FileOperation(std::shared_ptr<BlockManager> bm,
                             u64 max_inode_supported)
    : block_manager_(bm), inode_manager_(std::shared_ptr<InodeManager>(
                              new InodeManager(bm, max_inode_supported))),
      block_allocator_(std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, inode_manager_->get_reserved_blocks()))) {
  SuperBlock(bm, inode_manager_->get_max_inode_supported()).flush(0).unwrap();
}

auto FileOperation::create_from_raw(std::shared_ptr<BlockManager> bm)
    -> ChfsResult<std::shared_ptr<FileOperation>> {
  auto superblockResult = SuperBlock::create_from_existing(bm, 0);
  if (superblockResult.is_err())
  {
    return ChfsResult<std::shared_ptr<FileOperation>>(superblockResult.unwrap_error());
  }

  auto inodeManager = InodeManager::create_from_block_manager(bm, superblockResult.unwrap()->get_ninodes());
  if (inodeManager.is_err())
  {
    return ChfsResult<std::shared_ptr<FileOperation>>(inodeManager.unwrap_error());
  }

  auto reservedBlockCnt = inodeManager.unwrap().get_reserved_blocks();
  return ChfsResult<std::shared_ptr<FileOperation>>
  (std::shared_ptr<FileOperation>(new FileOperation(
          bm, InodeManager::to_shared_ptr(inodeManager.unwrap()),
          std::shared_ptr<BlockAllocator>(
              new BlockAllocator(bm, reservedBlockCnt, false)))));
}

auto FileOperation::get_free_inode_num() const -> ChfsResult<u64> {
  return inode_manager_->free_inode_cnt();
}

auto FileOperation::get_free_blocks_num() const -> ChfsResult<u64> {
  return ChfsResult<u64>(block_allocator_->free_block_cnt());
}

auto FileOperation::remove_file(inode_id_t id) -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();

  std::vector<u8> inode(block_size);

  std::vector<block_id_t> free_set;

  auto inodePtr = reinterpret_cast<Inode *>(inode.data());
  auto inodeResult = this->inode_manager_->read_inode(id, inode);
  if (inodeResult.is_err())
  {
    error_code = inodeResult.unwrap_error();
    goto err_ret;
  }

  for (uint i = 0; i < inodePtr->get_direct_block_num(); i++)
  {
    if (inodePtr->blocks[i] == KInvalidBlockID)
    {
      break;
    }
    free_set.push_back(inodePtr->blocks[i]);
  }

  if (inodePtr->blocks[inodePtr->get_direct_block_num()] != KInvalidBlockID)
  {
    std::vector<u8> indirect_block = {};
    auto readResult = this->block_manager_->read_block(inodePtr->blocks[inodePtr->get_direct_block_num()], indirect_block.data());
    if (readResult.is_err())
    {
      error_code = readResult.unwrap_error();
      goto err_ret;
    }

    auto blockArray = reinterpret_cast<block_id_t *>(indirect_block.data());
    for (uint i = 0; i < this->block_manager_->block_size() / sizeof(block_id_t); i++)
    {
      if (blockArray[i] == KInvalidBlockID)
      {
        break;
      }
      else
      {
        free_set.push_back(blockArray[i]);
      }
    }
  }


  {
    auto result = this->inode_manager_->free_inode(id);
    if (result.is_err())
    {
      error_code = result.unwrap_error();
      goto err_ret;
    }
    free_set.push_back(inodeResult.unwrap());
  }

  // now free the blocks
  for (auto bid : free_set)
  {
    auto result = this->block_allocator_->deallocate(bid);
    if (result.is_err())
    {
      return result;
    }
  }
  return KNullOk;
err_ret:
  return ChfsNullResult(error_code);
}

auto FileOperation::remove_file_atomic(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult
{
  auto error_code = ErrorType::DONE;
  const auto blockSize = this->block_manager_->block_size();

  std::vector<u8> inode(blockSize);

  std::vector<block_id_t> freeSet;

  Inode *inodePtr = reinterpret_cast<Inode *>(inode.data());
  auto inodeResult = this->inode_manager_->read_inode_from_memory(id, inode, tx_ops);
  if (inodeResult.is_err())
  {
    error_code = inodeResult.unwrap_error();
    goto err_ret;
  }

  for (uint i = 0; i < inodePtr->get_direct_block_num(); i++)
  {
    if (inodePtr->blocks[i] == KInvalidBlockID)
    {
      break;
    }
    freeSet.push_back(inodePtr->blocks[i]);
  }

  if (inodePtr->blocks[inodePtr->get_direct_block_num()] != KInvalidBlockID)
  {
    std::vector<u8> indirect_block = {};
    auto read_res = this->block_manager_->read_block_from_memory(inodePtr->blocks[inodePtr->get_direct_block_num()], indirect_block.data(), tx_ops);
    if (read_res.is_err())
    {
      error_code = read_res.unwrap_error();
      goto err_ret;
    }

    auto blockArray = reinterpret_cast<block_id_t *>(indirect_block.data());
    for (uint i = 0; i < this->block_manager_->block_size() / sizeof(block_id_t); i++)
    {
      if (blockArray[i] == KInvalidBlockID) {
        break;
      } else {
        freeSet.push_back(blockArray[i]);
      }
    }
  }
  {
    auto result = this->inode_manager_->free_inode_atomic(id, tx_ops);
    if (result.is_err())
    {
      error_code = result.unwrap_error();
      goto err_ret;
    }
    freeSet.push_back(inodeResult.unwrap());
  }

  for (auto bid : freeSet)
  {
    auto result = this->block_allocator_->deallocate_atomic(bid, tx_ops);
    if (result.is_err())
    {
      return result;
    }
  }
  return KNullOk;
  err_ret:
    return ChfsNullResult(error_code);
}
} // namespace chfs
