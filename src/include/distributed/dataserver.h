//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// dataserver.h
//
// Identification: src/include/distributed/dataserver.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "block/allocator.h"
#include "common/config.h"
#include "common/result.h"
#include "librpc/server.h"
#include <mutex>
namespace chfs {

/**
 * `DataServer` is slave server in chfs where the actual data are stored.
 *
 * In chfs, a file is split into one or more blocks and these blocks
 * are stored in a set of `DataServers`. It is responsible for serving
 * read and write requests from the client and some instructions like
 * block creation and deletion from the `MetadataServer`.
 */
class DataServer {

protected:
  const size_t num_worker_threads = 4; // worker threads for rpc handlers

  /**
   * The common logic in constructor
   */
  auto initialize(std::string const &data_path);

public:
  /**
   * Start a data server that listens on `localhost` with the given port.
   *
   * It receives requests from client and `MetadataServer` and controls
   * the blocks(fixed size file) which are actual data in chfs.
   *
   * @param port: The port number to listen on.
   * @param data_path: The file path where truly store data.
   */
  DataServer(u16 port, const std::string &data_path = "/tmp/block_data");

  /**
   * Start a data server listening on `address:port`.
   *
   * Notice that the `address` should be controlled by one of the
   * network adapters.
   *
   * @param address: The address to bind to.
   * @param port: The port number to listen to.
   * @param data_path: The file path where truly store data.
   */
  DataServer(const std::string &address, u16 port,
             const std::string &data_path = "/tmp/block_data");

  /**
   * Destructor. Close the rpc server gracefully.
   */
  ~DataServer();

  /** !!NOTE
   * It's kind of tricky since `rpclib` doesn't support
   * `std::variant` and these member functions in `Result`
   * also need to be implemented to be de/serializable by
   * `msgpack`. So we choose to make the client parse the
   * return value and return `ChfsResult`.
   */

  /**
   * A RPC handler for client. Read a piece of data from a given block.
   *
   * @param block_id: The block id allocated by metadata server.
   * @param offset: The offset inner the block that the client wants to read.
   * @param len: The length that client wants to read.
   * @param version: The version of the block to validate block map cache
   *
   * @return: Return the content in the block if valid, otherwise return
   * a 0-size vector(client wouldn't allow to read a data whose size is 0)
   */
  auto read_data(block_id_t block_id, usize offset, usize len,
                 version_t version) -> std::vector<u8>;

  /**
   * A RPC handler for client. Write a piece of data in a given block.
   *
   * @param block_id: The block id allocated by metadata server.
   * @param offset: The offset inner the block that the client wants to write.
   * @param buffer: The data client wants to write.
   *
   * @return: Whether the write request is valid or not
   */
  auto write_data(block_id_t block_id, usize offset, std::vector<u8> &buffer)
      -> bool;

  /**
   * A RPC handler for metadata server. Allocate a block on this server.
   *
   * @return: The block id which is allocated
   */
  auto alloc_block() -> std::pair<block_id_t, version_t>;

  /**
   * A RPC handler for metadata server. Delete an allocated block on the server.
   *
   * @param block_id: The block id.
   *
   * @return: Whether the delete request is valid or not
   */
  auto free_block(block_id_t block_id) -> bool;

 auto get_block_version(block_id_t block_id) -> version_t;

 auto update_block_version(block_id_t block_id) -> version_t;

private:
  std::unique_ptr<RpcServer> server_;
  std::shared_ptr<BlockAllocator> block_allocator_;
};

} // namespace chfs