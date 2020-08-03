package io.lubricant.consensus.raft.support.anomaly;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.context.member.Follower;
import io.lubricant.consensus.raft.support.RaftException;
import io.lubricant.consensus.raft.transport.RaftCluster;

/**
 * 非主异常（当前节点上的角色不是 Leader 时抛出）
 */
public class NotLeaderException extends RaftException {

    private final RaftCluster.ID currentLeader;

    public NotLeaderException(RaftParticipant participant) {
        if (participant instanceof Follower) {
            currentLeader = ((Follower)participant).currentLeader();
        } else {
            currentLeader = null;
        }
    }

    public RaftCluster.ID currentLeader() {
        return currentLeader;
    }

}