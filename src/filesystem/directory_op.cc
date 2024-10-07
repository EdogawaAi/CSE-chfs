#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(const std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  // UNIMPLEMENTED();
  src += ("/" + filename + ":" + std::to_string(id));
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::stringstream ss(src);
  std::string entry;

  while (getline(ss, entry, '/'))
  {
    auto delimiter = entry.find(':');
    if (delimiter != std::string::npos)
    {
      std::string name = entry.substr(0, delimiter);
      inode_id_t id = string_to_inode_id(entry.substr(delimiter + 1));
      list.push_back({name, id});
    }
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  // auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();
  std::stringstream ss(src);
  std::string entry;
  std::string result;

  while (getline(ss, entry, '/'))
  {
    if (entry.substr(0, entry.find(":")) != filename)
    {
      if (!result.empty())
      {
        result += '/';
      }
      result += entry;
    }
  }

  return result;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto read_result = fs->read_file(id);
  if (read_result.is_err())
  {
    return ChfsNullResult(read_result.unwrap_error());
  }

  auto data = read_result.unwrap();
  auto data_str = std::string(data.begin(), data.end());
  parse_directory(data_str, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  UNIMPLEMENTED();

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  UNIMPLEMENTED();

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  UNIMPLEMENTED();
  
  return KNullOk;
}

} // namespace chfs
