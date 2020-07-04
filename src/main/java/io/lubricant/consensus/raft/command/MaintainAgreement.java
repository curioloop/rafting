package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftLog.EntryKey;

import java.util.concurrent.TimeUnit;

/**
 * 状态维护策略
 */
public class MaintainAgreement {

    private static final long MIN_MAINTAIN_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    private long recentCommandCount;                // 状态机执行命令的数量（同线程访问）
    private volatile long minimalLogIndex;          // 最小的日志索引
    private volatile EntryKey lastSnapIncludeEntry; //
    private volatile EntryKey snapshotIncludeEntry; //

    private volatile boolean maintainInProgress;    // 维护任务进行中（已提交到线程池）
    private volatile boolean compactInProgress;     // 整理任务进行中（已提交到线程池）
    private volatile long lastMaintainSuccessTime;  // 最近一次维护成功的时间
    private volatile long lastMaintainTriggerTime;  // 最近一次触发维护的时间
    private volatile long lastCompactTriggerTime;   // 最近一次触发整理的时间

    private final long expectMaintainInterval; // 触发维护操作的间隔
    private final long expectCompactInterval;  // 触发整理操作的间隔
    private final long stateChangeThreshold;   // 当状态机执行的命令数量超过该值，触发清理
    private final long dirtyLogTolerance;      // 当已经提交的日志数量超过该值，触发清理

    public MaintainAgreement() {
        this(TimeUnit.MINUTES.toMillis(5), TimeUnit.MINUTES.toMillis(5),1000, 5000);
    }

    public MaintainAgreement(long expectMaintainInterval, long expectCompactInterval, long stateChangeThreshold, long dirtyLogTolerance) {
        this.expectMaintainInterval = Math.max(expectMaintainInterval, MIN_MAINTAIN_INTERVAL);
        this.expectCompactInterval = Math.max(expectCompactInterval, expectMaintainInterval);
        this.stateChangeThreshold = stateChangeThreshold;
        this.dirtyLogTolerance = dirtyLogTolerance;
        this.lastMaintainTriggerTime = System.currentTimeMillis();
        this.lastCompactTriggerTime = System.currentTimeMillis();
    }

    public void increaseCommand() {
        recentCommandCount++;
    }

    public void minimalLogIndex(long minimalLogIndex) {
        if (minimalLogIndex < this.minimalLogIndex) {
            throw new AssertionError(String.format(
                    "log index decrease %d < %d", minimalLogIndex, this.minimalLogIndex));
        }
        this.minimalLogIndex = minimalLogIndex;
    }

    public long minimalLogIndex() {
        return this.minimalLogIndex;
    }

    public void snapshotIncludeEntry(long index, long term) {
        if (lastSnapIncludeEntry != null) {
            if (lastSnapIncludeEntry.index() > index) {
                throw new AssertionError(String.format(
                        "snapshot index decrease %d < %d", index, lastSnapIncludeEntry.index()));
            } else if (lastSnapIncludeEntry.index() == index) {
                return;
            }
        }
        snapshotIncludeEntry = new EntryKey(index, term);
    }

    public Entry snapshotIncludeEntry() {
        return snapshotIncludeEntry;
    }

    public boolean needMaintain(long lastAppliedIndex) {
        if (maintainInProgress) {
            return false;
        }
        long currentMills = System.currentTimeMillis();
        if (currentMills - lastMaintainTriggerTime < MIN_MAINTAIN_INTERVAL) {
            return false;
        }
        if (currentMills - lastMaintainSuccessTime < expectMaintainInterval) {
            return false;
        }
        if (lastAppliedIndex - minimalLogIndex < dirtyLogTolerance) {
            return false;
        }
        if (recentCommandCount < stateChangeThreshold) {
            return false;
        }
        return false;
    }

    public void triggerMaintenance() {
        maintainInProgress = true;
    }

    public void finishMaintenance(boolean success) {
        if (success) {
            lastMaintainSuccessTime = System.currentTimeMillis();
            recentCommandCount = 0;
        }
        lastMaintainTriggerTime = System.currentTimeMillis();
        maintainInProgress = false;
    }

    public boolean needCompact() {
        return ! compactInProgress && snapshotIncludeEntry != null &&
        System.currentTimeMillis() - lastCompactTriggerTime > expectCompactInterval;
    }

    public void triggerCompaction() {
        compactInProgress = true;
    }

    public void finishCompaction(boolean success) {
        if (success) {
            lastSnapIncludeEntry = snapshotIncludeEntry;
            snapshotIncludeEntry = null;
        }
        lastCompactTriggerTime = System.currentTimeMillis();
        compactInProgress = false;
    }
}
