#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
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
  std::ostringstream oss;

  if (!src.empty())
  {
    oss << '/';
  }

  oss << filename << ':' << id;
  src.append(oss.str());
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  list.clear();
  std::istringstream iss(src);
  std::string token;
  while (getline(iss, token, '/'))
  {
    std::istringstream tokenStream(token);
    DirectoryEntry dir;
    if (getline(tokenStream, dir.name, ':') && tokenStream >> dir.id)
    {
      list.push_back(dir);
    } else
    {
      std::cerr << "Error parsing directory entry: " << src << std::endl;
    }
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();
  std::list<DirectoryEntry> rawList;
  parse_directory(src, rawList);
  auto iter = rawList.begin();
  while (iter != rawList.end())
  {
    if ((*iter).name == filename)
    {
      auto nextIter = ++iter;
      rawList.erase(--iter);
      iter = nextIter;
      continue;
    }
    iter++;
  }
  res = dir_list_to_string(rawList);
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  std::vector<u8> directories = (fs->read_file(id)).unwrap();
  std::string dirString = std::string(directories.begin(), directories.end());
  parse_directory(dirString, list);
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
