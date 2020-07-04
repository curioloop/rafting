package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;

/**
 * 成员基础信息
 */
public abstract class RaftMember implements RaftParticipant {

    protected final RaftContext ctx;
    protected final Membership membership;

    // 响应 RPC 前需要持久化以下两个属性
    protected final long currentTerm; // 已知的最新的 term（初始为 0）
    protected final ID lastCandidate; // 最近一次赞成投票的 candidate

    protected RaftMember(RaftContext context, long term, ID candidate, Membership membership) {
        this.ctx = context;
        this.currentTerm = term;
        this.lastCandidate = candidate;
        this.membership = membership;
        ctx.stableStorage().persist(currentTerm, lastCandidate);
    }

    /**
     * 是否处于绑定线程中
     */
    protected void assertEventLoop() {
        if (! ctx.inEventLoop()) {
            throw new AssertionError("not in event loop");
        }
    }

    /**
     * 当前成员的信息
     */
    public Membership membership() {
        return membership;
    }

    /**
     *  @see RaftParticipant#currentTerm()
     */
    @Override
    public long currentTerm() {
        return currentTerm;
    }

    /**
     *  @see RaftParticipant#votedFor()
     */
    @Override
    public ID votedFor() {
        return lastCandidate;
    }

    @Override
    public RaftResponse installSnapshot(long term, ID leaderId, long lastIncludedIndex, long lastIncludedTerm) throws Exception {
        if (term >= currentTerm) {
            throw new AssertionError("leader invoke InstallSnapshot before AppendEntries");
        }
        return RaftResponse.failure(currentTerm);
    }
}
