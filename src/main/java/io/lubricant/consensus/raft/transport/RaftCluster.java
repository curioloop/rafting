package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.support.PendingTask;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;

import java.io.Serializable;
import java.util.Set;

/**
 * 集群
 */
public interface RaftCluster extends AutoCloseable {

    /**
     * 节点 ID
     */
    interface ID extends Serializable {}

    /**
     * 快照服务
     */
    interface SnapService {
        PendingTask<Snapshot> obtainSnapshot(long lastIncludedIndex, long lastIncludedTerm);
    }

    /**
     * 集群大小
     */
    int size();

    /**
     * 本地节点 ID
     */
    ID localID();

    /**
     * 远程节点 ID （数量固定的集合）
     */
    Set<ID> remoteIDs();

    /**
     * 远程节点（服务不可用时可返回空）
     */
    <Service extends RaftService & SnapService> Service remoteService(ID id, String ctxID);

}
