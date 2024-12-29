#pragma once

#include <list>

#include "./operations.h"

// Helper functions to add a directory layer atop of the file

namespace chfs {

    struct DirectoryEntry {
        std::string name;
        inode_id_t id;
    };

    /**
     * Read the directory information and convert it to a string
     */
    auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
        -> std::string;

    /**
     * Parse a directory of content "name0:inode0/name1:inode1/ ..."
     * in the list.
     *
     * # Warn
     * Note that, we assume the file name will not contain '/'
     *
     * @param src: the string to parse
     * @param list: the list to store the parsed content
     */
    void parse_directory(std::string &src, std::list<DirectoryEntry> &list);

    /**
     * Append a new entry to the directory.
     *
     * @param src: the string to append to
     * @param filename: the filename to append
     * @param id: the inode id to append
     */
    auto append_to_directory(std::string src, std::string filename, inode_id_t id)
        -> std::string;

    /**
     * Remove an entry from the directory.
     *
     * @param src: the string to remove from
     * @param filename: the filename to remove
     */
    auto rm_from_directory(std::string src, std::string filename) -> std::string;

    /**
     * Read the directory information.
     * We assume that the directory information is stored as a
     * "name0:inode0/name1:inode1/ ..." string in the file blocks.
     *
     * @param fs: the pointer to the file system
     * @param inode: the inode number of the directory
     * @param list: the list to store the read content
     *
     * ## Warn: we don't check whether the inode is a directory or not
     * We assume the upper layer should handle this
     */
    auto read_directory(FileOperation *fs, inode_id_t inode,
                        std::list<DirectoryEntry> &list) -> ChfsNullResult;

} // namespace chfs