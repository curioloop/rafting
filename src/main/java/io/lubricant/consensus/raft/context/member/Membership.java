package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;

import java.util.Objects;

/**
 * 参与者信息（用于判断是否需要进行角色转换）
 */
public class Membership {

    // 两个重要原则：
    //  - 当接收到 term >= currentTerm 的 appendEntries 请求时，当前 candidate 转换为 follower
    //  - 当接收到足够的选票时，candidate 转换为 currentTerm 对应的 leader
    //
    // 重点关注 term 相同的冲突情况如何处理：
    //
    // 如果某个节点（已经/将要）转换为 candidate 并发起一轮 term = n 的选举
    // 但是此时接收到一条来自 term = n 的 appendEntries 请求
    // 此时应放弃（尚未/进行中）选举，转换为 Follower
    // 转换优先级：follower > candidate
    //
    // 如果某个节点在 term = m 的选举中已经收到了足够的选票，（已经/将要）成为 leader
    // 此时不可能接收到 term = m 的其他 leader 发送来的心跳信息
    //
    private final long term;
    private final Class<? extends RaftParticipant> role;
    private final ID ballot;
    private final boolean applied;

    /**
     * 成员转换信息
     * @param role 将要转换的类型
     * @param term currentTerm
     * @param ballot candidateID
     */
    public Membership(Class<? extends RaftParticipant> role, long term, ID ballot) {
        this(role, term, ballot, false);
    }

    public Membership(Membership membership) {
        this(membership.role, membership.term, membership.ballot, true);
    }

    private Membership(Class<? extends RaftParticipant> role, long term, ID ballot, boolean applied) {
        if (role != Follower.class && role != Candidate.class && role != Leader.class)
            throw new IllegalArgumentException("unsupported raft participant");
        this.role = Objects.requireNonNull(role);
        this.term = term;
        this.ballot = ballot;
        this.applied = applied;
    }

    public Class<? extends RaftParticipant> role() {
        return role;
    }

    public long term() {
        return term;
    }

    public ID ballot() {
        return ballot;
    }

    public boolean notApplied() {
        return ! applied;
    }

    /**
     * 判断是否有必要进行转换
     */
    public boolean isBetter(Membership o) {
        if (o == null) {
            return true;
        }

        // greater term is preferred
        if (term != o.term) {
            return term > o.term;
        }

        if (role != o.role) {
            if (role == Leader.class) {
                if (o.role == Candidate.class) {
                    return true; // candidate win the election
                } else {
                    throw new AssertionError("leader should remain unchanged during the same term");
                }
            }
            // if received RPC from candidate or leader then turn to follower firstly instead of candidate
            return role == Follower.class;
        } else {
            if (role == Leader.class) {
                return false; // just keep the leadership, don`t need to change
            }
            if (role == Follower.class) {
                return true; // both convert to the same term follower，may pre-vote fail
            }
        }

        if (! ballot.equals(o.ballot)) {
            throw new AssertionError("candidate should vote to itself");
        }

        return false; // both convert to the same term candidate，just ignore
    }

}
