package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftLog.EntryKey;
import io.lubricant.consensus.raft.support.RaftConfig;

import java.util.concurrent.TimeUnit;

/**
 * 状态维护策略
 */
public class MaintainAgreement {

    private static final long MIN_TRIGGER_INTERVAL = TimeUnit.SECONDS.toMillis(5);
    private static final long MIN_MAINTAIN_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    private long recentCommandCount;                // 状态机执行命令的数量（同线程访问）
    private volatile long minimalLogIndex;          // 最小的日志索引
    private volatile EntryKey lastSnapIncludeEntry; //
    private volatile EntryKey snapshotIncludeEntry; //

    private volatile boolean maintainInProgress;    // 维护任务进行中（已提交到线程池）
    private volatile boolean compactInProgress;     // 整理任务进行中（已提交到线程池）
    private volatile long lastMaintainSuccessTime;  // 最近一次维护成功的时间
    private volatile long lastMaintainTriggerTime;  // 最近一次触发维护的时间
    private volatile long lastCompactSuccessTime;   // 最近一次整理成功的时间
    private volatile long lastCompactTriggerTime;   // 最近一次触发整理的时间

    private final long expectTriggerInterval;  // 操作触发的间隔
    private final long expectMaintainInterval; // 维护状态机的间隔
    private final long expectCompactInterval;  // 整理日志的间隔
    private final long stateChangeThreshold;   // 当状态机执行的命令数量超过该值，触发清理
    private final long dirtyLogTolerance;      // 当已经提交的日志数量超过该值，触发清理

    public MaintainAgreement(RaftConfig config) {
        this(Math.max(config.snapTriggerInterval(), MIN_TRIGGER_INTERVAL),
             Math.max(config.snapMaintainInterval(), MIN_MAINTAIN_INTERVAL),
             config.snapCompactInterval(), config.snapStateChangeThreshold(), config.snapDirtyLogTolerance());
    }

    public MaintainAgreement(long expectTriggerInterval, long expectMaintainInterval, long expectCompactInterval, int stateChangeThreshold, int dirtyLogTolerance) {
        this.expectTriggerInterval = expectTriggerInterval;
        this.expectMaintainInterval = expectMaintainInterval;
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
            if (lastSnapIncludeEntry.index() > index ||
                lastSnapIncludeEntry.term() > term) {
                throw new AssertionError(String.format(
                        "snapshot index decrease (%d:%d) < (%d:%d)", index, term,
                        lastSnapIncludeEntry.index(), lastSnapIncludeEntry.term()));
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
        if (currentMills - lastMaintainTriggerTime < expectTriggerInterval) {
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
        return true;
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
        if (compactInProgress || snapshotIncludeEntry == null) {
            return false;
        }
        long currentMills = System.currentTimeMillis();
        if (currentMills - lastCompactTriggerTime < expectTriggerInterval) {
            return false;
        }
        if (currentMills - lastCompactSuccessTime < expectCompactInterval) {
            return false;
        }
        return true;
    }

    public void triggerCompaction() {
        compactInProgress = true;
    }

    public void finishCompaction(boolean success) {
        if (success) {
            lastCompactSuccessTime = System.currentTimeMillis();
            lastSnapIncludeEntry = snapshotIncludeEntry;
            snapshotIncludeEntry = null;
        }
        lastCompactTriggerTime = System.currentTimeMillis();
        compactInProgress = false;
    }
}
