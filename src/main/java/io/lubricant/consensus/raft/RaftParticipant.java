package io.lubricant.consensus.raft;

import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.command.RaftLog.Entry;

/**
 * 参与者
 */
public interface RaftParticipant {

    /**
     * 已知的最新任期
     */
    long currentTerm();

    /**
     * 在当前任期第一个知悉的 candidate ID
     */
    ID votedFor();

    /**
     * 超时响应
     */
    default void onTimeout() {}

    /**
     * 挤出响应
     */
    default void onFencing() {}

    /**
     * @see RaftService#appendEntries(long, ID, long, long, Entry[], long)
     */
    RaftResponse appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception;

    /**
     * @see RaftService#preVote(long, ID, long, long)
     */
    RaftResponse preVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception;

    /**
     * @see RaftService#requestVote(long, ID, long, long)
     */
    RaftResponse requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception;

    /**
     * @see RaftService#installSnapshot(long, ID, long, long)
     */
    RaftResponse installSnapshot(long term, ID leaderId, long lastIncludedIndex, long lastIncludedTerm) throws Exception;

}
