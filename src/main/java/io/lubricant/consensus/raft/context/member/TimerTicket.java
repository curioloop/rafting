package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时器存根（保存当前的处理状态）
 */
public class TimerTicket extends AtomicLong {

    public final static long TIMEOUT = -1; // 超时
    public final static long FENCING = -2; // 挤出
    public final static long INVALID = -3; // 失效

    private final Membership membership;
    private final RaftParticipant participant;
    private volatile ScheduledFuture schedule;

    public TimerTicket(RaftParticipant participant, long deadline) {
        super(deadline);
        this.membership = ((RaftMember) participant).membership();
        this.participant = participant;
    }

    public long term() {
        return participant.currentTerm();
    }

    public AtomicLong deadline() {
        return this;
    }

    public RaftParticipant participant() {
        return participant;
    }

    public Membership membership() {
        return membership;
    }

    public ScheduledFuture schedule() {
        return schedule;
    }

    public TimerTicket with(ScheduledFuture future) {
        this.schedule = future;
        return this;
    }

}
