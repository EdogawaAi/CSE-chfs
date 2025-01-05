#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int voted_for;
    int received_votes;
    int commit_index;
    int last_applied;

    unsigned long last_received_timestamp;
    std::unique_ptr<int[]> next_index;
    std::unique_ptr<int[]> match_index;

    std::vector<LogEntry<Command>> log_entry_list;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1),
    voted_for(-1),
    received_votes(0),
    commit_index(0),
    last_applied(0)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */
    state = std::make_unique<StateMachine>();
    thread_pool = std::make_unique<ThreadPool>(32);
    last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    next_index.reset(new int[configs.size()]);
    match_index.reset(new int[configs.size()]);

    //first log index is 1
    //Dummy log
    LogEntry<Command> dummy_entry;
    dummy_entry.term = 0;
    dummy_entry.index = 0;
    log_entry_list.push_back(dummy_entry);

    //Persistent state on all servers
    std::string node_log_filename = "/tmp/raft_log/node" + std::to_string(node_id);
    bool is_recover = is_file_exist(node_log_filename);
    auto bm_ = std::shared_ptr<BlockManager>(new BlockManager(node_log_filename));
    log_storage = std::make_unique<RaftLog<Command>>(bm_, is_recover);
    if (is_recover)
    {
        log_storage->recover(current_term, voted_for, log_entry_list);
    } else
    {
        log_storage->updateMetaData(current_term, voted_for);
        log_storage->updateLogs(log_entry_list);
    }

    RAFT_LOG("Init a raft node");
    rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    next_index.reset();
    match_index.reset();
    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    RAFT_LOG("Start a raft node");
    for (auto iter = node_configs.begin(); iter != node_configs.end(); iter++)
    {
        rpc_clients_map.insert(std::make_pair(iter->node_id, std::make_unique<RpcClient>(iter->ip_address, iter->port, true)));
    }
    stopped.store(false);

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
    RAFT_LOG("Raft node started successfully");

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped.store(true);
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    if (role == RaftRole::Leader)
    {
        return std::make_tuple(true, current_term);
    }
    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if (role != RaftRole::Leader)
    {
        return std::make_tuple(false, current_term, log_entry_list.size());
    }
    else
    {
        LogEntry<Command> logEntry;
        logEntry.index = log_entry_list.size();
        logEntry.term = current_term;
        Command command;
        int command_size = command.size();
        command.deserialize(cmd_data, command_size);
        logEntry.command = command;
        log_entry_list.push_back(logEntry);
        log_storage->updateLogs(log_entry_list);
        return std::make_tuple(true, current_term, logEntry.index);
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if (args.term < current_term || (args.term == current_term && voted_for != -1 && voted_for != args.candidateId))
    {
        RequestVoteReply reply;
        reply.voteFollowerId = my_id;
        reply.term = current_term;
        reply.voteGranted = false;
        return reply;
    }
    role = RaftRole::Follower;
    current_term = args.term;
    voted_for = -1;
    if (log_entry_list.back().term > args.lastLogTerm || (log_entry_list.back().term == args.lastLogTerm && (log_entry_list.size() - 1) > args.lastLogIndex))
    {
        RAFT_LOG("Refuse vote request from node %d", args.candidateId);
        RequestVoteReply reply;
        reply.voteFollowerId = my_id;
        reply.term = current_term;
        reply.voteGranted = false;

        log_storage->updateMetaData(current_term, voted_for);
        return reply;
    }

    voted_for = args.candidateId;
    RequestVoteReply reply;
    reply.voteFollowerId = my_id;
    reply.term = current_term;
    reply.voteGranted = true;
    last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    leader_id = args.candidateId;

    log_storage->updateMetaData(current_term, voted_for);

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    const auto MAC_NUM = rpc_clients_map.size();
    if (reply.term > current_term)
    {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        log_storage->updateMetaData(current_term, voted_for);
        return;
    }
    if (role != RaftRole::Candidate || arg.term < current_term)
    {
        return;
    }
    if (reply.voteGranted)
    {
        received_votes++;
        if (received_votes >= MAC_NUM / 2 + 1)
        {
            RAFT_LOG("New leader is node %d", my_id);
            role = RaftRole::Leader;

            for (int i = 0; i < node_configs.size(); i++)
            {
                next_index[i] = log_entry_list.size();
                match_index[i] = 0;
            }
        }
    }
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    bool is_meta_changed = false;
    bool is_log_changed = false;
    AppendEntriesReply reply;
    auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
    if (leader_id == arg.leaderId)
    {
        last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    }

    if (arg.term > current_term)
    {
        if (leader_id != arg.leaderId)
        {
            leader_id = arg.leaderId;
            last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
        }
        role = RaftRole::Follower;
        current_term = arg.term;
        voted_for = -1;
        is_meta_changed = true;

        if (log_entry_list.size() - 1 < arg.prevLogIndex || log_entry_list[arg.prevLogIndex].term != arg.prevLogTerm)
        {
            reply.term = current_term;
            reply.success = false;
        }
        else
        {
            log_entry_list.resize(arg.prevLogIndex + 1);
            std::vector<LogEntry<Command>> new_entry_list = arg.logEntryList;
            for (const auto &entry : new_entry_list)
            {
                log_entry_list.push_back(entry);
            }

            is_log_changed = true;
            int last_new_entry_index = log_entry_list.size() - 1;
            if (arg.leaderCommit > commit_index)
            {
                commit_index = std::min(arg.leaderCommit, last_new_entry_index);
            }
            reply.term = current_term;
            reply.success = true;
        }
    }
    else if (arg.term == current_term)
    {
        if (leader_id != arg.leaderId)
        {
            leader_id = arg.leaderId;
            last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
        }
        role = RaftRole::Follower;
        if (log_entry_list.size() - 1 < arg.prevLogIndex || log_entry_list[arg.prevLogIndex].term != arg.prevLogTerm)
        {
            reply.term = current_term;
            reply.success = false;
        }
        else
        {
            log_entry_list.resize(arg.prevLogIndex + 1);
            std::vector<LogEntry<Command>> new_entry_list = arg.logEntryList;
            for (const auto &entry : new_entry_list)
            {
                log_entry_list.push_back(entry);
            }
            is_log_changed = true;

            int last_new_entry_index = log_entry_list.size() - 1;
            if (arg.leaderCommit > commit_index)
            {
                commit_index = std::min(arg.leaderCommit, last_new_entry_index);
            }
            reply.term = current_term;
            reply.success = true;
        }
    }
    else
    {
        reply.term = current_term;
        reply.success = false;
    }

    if (is_meta_changed)
    {
        log_storage->updateMetaData(current_term, voted_for);
    }
    if (is_log_changed)
    {
        log_storage->updateLogs(log_entry_list);
    }
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    if (reply.term > current_term)
    {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        RAFT_LOG("Follower %d becomes leader", my_id);
        log_storage->updateMetaData(current_term, voted_for);
        return;
    }
    if (role != RaftRole::Leader)
    {
        return;
    }
    if (reply.success == false)
    {
        if (reply.term > arg.term)
        {
            return;
        }
        else
        {
            next_index[node_id] = next_index[node_id] - 1;
        }
    }
    else
    {
        int reply_match_index = arg.prevLogIndex + arg.logEntryList.size();
        if (reply_match_index > match_index[node_id])
        {
            match_index[node_id] = arg.prevLogIndex + arg.logEntryList.size();
        }
        next_index[node_id] = match_index[node_id] + 1;

        const auto MAC_NUM = rpc_clients_map.size();
        const auto LAST_INDEX = log_entry_list.size() - 1;
        for (int N = LAST_INDEX; N > commit_index; N--)
        {
            if (log_entry_list[N].term != current_term)
            {
                break;
            }
            int num_of_matched = 1;
            for (auto iter = rpc_clients_map.begin(); iter != rpc_clients_map.end(); iter++)
            {
                if (iter->first == my_id)
                {
                    continue;
                }
                if (match_index[iter->first] >= N)
                {
                    num_of_matched++;
                }
                if (num_of_matched >= MAC_NUM / 2 + 1)
                {
                    commit_index = N;
                    break;
                }
            }
            if (commit_index == N)
            {
                break;
            }
        }
    }
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    RAFT_LOG("Background election started");
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis1(300, 600);
            std::uniform_int_distribution<> dis2(1000, 1300);

            const unsigned long TIMEOUT_LIMIT_FOLLOWER = dis1(gen);
            const unsigned long TIMEOUT_LIMIT_CANDIDATE = dis2(gen);
            unsigned long current_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();

            if ((role == RaftRole::Follower && current_time - last_received_timestamp > TIMEOUT_LIMIT_FOLLOWER) || (role == RaftRole::Candidate && current_time - last_received_timestamp > TIMEOUT_LIMIT_CANDIDATE))
            {
                if (role != RaftRole::Candidate)
                {
                    RAFT_LOG("Candidate %d starts election", my_id);
                }
                role = RaftRole::Candidate;
                current_term++;
                received_votes = 1;
                voted_for = my_id;
                last_received_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
                for (auto iter = rpc_clients_map.begin(); iter != rpc_clients_map.end(); iter++)
                {
                    int target_id = iter->first;
                    if (target_id == my_id)
                    {
                        continue;
                    }
                    RequestVoteArgs vote_request;
                    vote_request.term = current_term;
                    vote_request.candidateId = my_id;
                    vote_request.lastLogIndex = log_entry_list.size() - 1;
                    vote_request.lastLogTerm = log_entry_list[vote_request.lastLogIndex].term;
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, target_id, vote_request);
                }
                log_storage->updateMetaData(current_term, voted_for);
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if (role == RaftRole::Leader)
            {
                for (auto iter = rpc_clients_map.begin(); iter != rpc_clients_map.end(); iter++)
                {
                    if (iter->first == my_id)
                    {
                        continue;
                    }
                    if (next_index[iter->first] < log_entry_list.size())
                    {
                        std::vector<LogEntry<Command>> append_entry_list;
                        append_entry_list.clear();
                        for (int i = next_index[iter->first]; i < log_entry_list.size(); i++)
                        {
                            append_entry_list.push_back(log_entry_list[i]);
                        }
                        AppendEntriesArgs<Command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.prevLogIndex = next_index[iter->first] - 1;
                        args.prevLogTerm = log_entry_list[args.prevLogIndex].term;
                        args.leaderCommit = commit_index;
                        args.logEntryList = append_entry_list;
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, iter->first, args);
                    }
                }
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if (last_applied < commit_index)
            {
                for (int i = last_applied + 1; i <= commit_index; i++)
                {
                    state->apply_log(log_entry_list[i].command);
                }
                last_applied = commit_index;
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if (role == RaftRole::Leader)
            {
                for (auto iter = rpc_clients_map.begin(); iter != rpc_clients_map.end(); iter++)
                {
                    int target_id = iter->first;
                    if (target_id == my_id)
                    {
                        continue;
                    }
                    AppendEntriesArgs<Command> heartbeat;
                    heartbeat.term = current_term;
                    heartbeat.leaderId = my_id;
                    heartbeat.prevLogIndex = next_index[iter->first] - 1;
                    heartbeat.prevLogTerm = log_entry_list[heartbeat.prevLogIndex].term;
                    heartbeat.leaderCommit = commit_index;
                    heartbeat.logEntryList.clear();
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, target_id, heartbeat);
                }
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}