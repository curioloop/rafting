package io.lubricant.consensus.raft;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.rpc.Async;

/**
 * RPC 接口
 * */
public interface RaftService {

    /**
     * 复制日志+发送心跳（由 leader 调用）
     * @param term leader 任期
     * @param leaderId leader 在集群中的唯一标识
     * @param prevLogIndex 紧接在新的之前的日志条目索引
     * @param prevLogTerm prevLogIndex 对应的任期
     * @param entries 日志条目（发送心跳时为空）
     * @param leaderCommit leader 已经提交的日志条目索引
     * @return 当 follower 中的日志包含 prevLogIndex 与 prevLogTerm 匹配的日志条目返回 true
     * */
    Async<RaftResponse> appendEntries(
            long term, ID leaderId,
            long prevLogIndex, long prevLogTerm,
            Entry[] entries, long leaderCommit) throws Exception;

    /**
     * 预选（由 follower 调用）
     * @param term candidate 任期
     * @param candidateId candidate 在集群中的唯一标识
     * @param lastLogIndex candidate 最后一条日志条目的索引
     * @param lastLogTerm  candidate 最后一条日志条目的任期
     * @return 当收到赞成票时返回 true
     * */
    Async<RaftResponse> preVote(
            long term, ID candidateId,
            long lastLogIndex, long lastLogTerm) throws Exception;

    /**
     * 选主（由 candidate 调用）
     * @param term candidate 任期
     * @param candidateId candidate 在集群中的唯一标识
     * @param lastLogIndex candidate 最后一条日志条目的索引
     * @param lastLogTerm  candidate 最后一条日志条目的任期
     * @return 当收到赞成票时返回 true
     * */
    Async<RaftResponse> requestVote(
            long term, ID candidateId,
            long lastLogIndex, long lastLogTerm) throws Exception;

    /**
     * 同步快照（由 leader 调用）
     * @param term leader 任期
     * @param leaderId leader 在集群中的唯一标识
     * @param lastIncludedIndex 快照将替换所有条目，直至包括该索引
     * @param lastIncludedTerm lastIncludedIndex 对应的任期
     * @return 当同步完成时返回 true
     */
    Async<RaftResponse> installSnapshot(
            long term, ID leaderId,
            long lastIncludedIndex, long lastIncludedTerm) throws Exception;
}
