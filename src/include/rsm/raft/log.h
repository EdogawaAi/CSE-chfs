#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "filesystem/operations.h"
#include "rsm/raft/protocol.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm, bool is_recover);
    ~RaftLog();

    /* Lab3: Your code here */
    void updateMetaData(int current_term, int voted_for);
    void updateLogs(std::vector<LogEntry<Command>> &logs);
    void recover(int &current_term, int &voted_for, std::vector<LogEntry<Command>> &logs);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::shared_ptr<FileOperation> fs_;

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, bool is_recover)
{
    /* Lab3: Your code here */
    bm_ = bm;

    const int MAX_INODE_NUM = 16;
    if (is_recover)
    {
        auto res = FileOperation::create_from_raw(bm_);
        if (res.is_err())
        {
            std::cout << "Failed to recover log file" << std::endl;
        }
        fs_ = res.unwrap();
    }
    else
    {
        fs_.reset(new FileOperation(bm_, MAX_INODE_NUM));
        auto meta_res = fs_->alloc_inode(InodeType::FILE);
        if (meta_res.is_err() || meta_res.unwrap() != 1)
        {
            std::cout << "Failed to init metadata file" << std::endl;
        }
        auto log_res = fs_->alloc_inode(InodeType::FILE);
        if (log_res.is_err() || log_res.unwrap() != 2)
        {
            std::cout << "Failed to init log file" << std::endl;
        }
    }
}

template <typename Command>
void RaftLog<Command>::updateMetaData(int current_term, int voted_for)
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<int> meta;
    meta.push_back(current_term);
    meta.push_back(voted_for);
    auto data = reinterpret_cast<u8 *>(meta.data());
    std::vector<u8> data_(data, data + sizeof(int) * 2);
    fs_->write_file(1, data_);
}

template <typename Command>
void RaftLog<Command>::updateLogs(std::vector<LogEntry<Command>>& logs)
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> data_;
    for (auto &log : logs)
    {
        auto log_data = reinterpret_cast<u8 *>(&log);
        data_.insert(data_.end(), log_data, log_data + sizeof(LogEntry<Command>));
    }
    fs_->write_file(2, data_);
}

template <typename Command>
void RaftLog<Command>::recover(int& current_term, int& voted_for, std::vector<LogEntry<Command>>& logs)
{
    std::unique_lock<std::mutex> lock(mtx);
    auto meta_res = fs_->read_file(1);
    auto log_res = fs_->read_file(2);
    if (meta_res.is_err() || log_res.is_err())
    {
        std::cout << "Failed to recover metadata or logs" << std::endl;
    }
    auto meta_data = meta_res.unwrap();
    auto log_data = log_res.unwrap();

    auto meta = reinterpret_cast<int *>(meta_data.data());
    current_term = meta[0];
    voted_for = meta[1];

    // recover logs
    int log_size = log_data.size();
    int log_entry_num = log_size / sizeof(LogEntry<Command>);
    auto log_entry_data = reinterpret_cast<LogEntry<Command> *>(log_data.data());
    logs.clear();
    for (int i = 0; i < log_entry_num; i++)
    {
        logs.push_back(log_entry_data[i]);
    }
}


template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
