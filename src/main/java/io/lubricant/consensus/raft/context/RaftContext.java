package io.lubricant.consensus.raft.context;

import io.lubricant.consensus.raft.command.MaintainAgreement;
import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.SnapshotArchive;
import io.lubricant.consensus.raft.context.member.Follower;
import io.lubricant.consensus.raft.context.member.Membership;
import io.lubricant.consensus.raft.context.member.TimerTicket;
import io.lubricant.consensus.raft.support.*;
import io.lubricant.consensus.raft.support.EventLoop.ContextEventLoop;
import io.lubricant.consensus.raft.support.StableLock.Persistence;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.support.anomaly.ObsoleteContextException;
import io.lubricant.consensus.raft.transport.RaftCluster;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.RaftCluster.SnapService;
import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftLog.EntryKey;
import io.lubricant.consensus.raft.command.RaftMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 上下文状态（用于保存/传递状态数据）
 */
public class RaftContext {

    private final Logger logger = LoggerFactory.getLogger(RaftContext.class);

    private final String id;
    private RaftConfig envConfig; // 环境配置
    private RaftLog replicatedLog; // 复制日志
    private RaftMachine stateMachine; // 状态机
    private StableLock stableStorage; // 持久化文件
    private SnapshotArchive snapArchive; // 快照档案

    private RaftCluster cluster; // 集群
    private RaftRoutine routine; // 例程
    private ContextEventLoop eventLoop; // 事件循环

    private Map<EntryKey, Promise> commandPromises = new ConcurrentHashMap<>();

    final AtomicReference<TimerTicket> ticketHolder = new AtomicReference<>(); // 保存最新的状态
    final AtomicReference<Membership> membershipFilter = new AtomicReference<>(); // 支持抢占切换

    final AtomicInteger commitVersion = new AtomicInteger();
    final LightweightMutex commandMutex = new LightweightMutex();

    final MaintainAgreement maintainAgreement;
    PendingTask<Void> snapshotInstallation;

    volatile boolean stillRunning = true;

    public RaftContext(String id, StableLock lock, SnapshotArchive snap, RaftConfig config, RaftLog log, RaftMachine machine) throws Exception {
        this.id = id;
        this.envConfig = config;
        this.replicatedLog = log;
        this.stateMachine = machine;
        this.stableStorage = lock;
        this.snapArchive = snap;

        Snapshot snapshot = snapArchive.lastSnapshot();
        long snapIndex = 0, snapTerm = 0;
        if (snapshot != null) {
            snapIndex = snapshot.lastIncludeIndex();
            snapTerm = snapshot.lastIncludeTerm();
        }

        Entry epoch = replicatedLog.epoch();
        boolean catchUp = snapshot == null ? epoch.index() == 0 && epoch.term() == 0 :
                snapIndex >= epoch.index() && snapTerm >= epoch.term();
        if (! catchUp) {
            throw new AssertionError(String.format("snapshot(%d:%d) not catch up with log(%d:%d)",
                    snapIndex, snapTerm, epoch.index(), epoch.term()));
        }

        maintainAgreement = new MaintainAgreement(config);
        maintainAgreement.minimalLogIndex(epoch.index());
    }

    public Promise<Void> initialize(RaftCluster cluster, RaftRoutine routine, ContextEventLoop eventLoop) {
        this.cluster = cluster;
        this.routine = routine;
        this.eventLoop = eventLoop;

        Promise<Void> promise = new Promise<>();
        eventLoop.execute(() -> {
            try {
                Persistence restore = stableStorage.restore();
                Entry epoch = replicatedLog.epoch();
                Entry milestone = restore.milestone;
                if (epoch.index() < milestone.index()) {
                    logger.warn("RaftCtx({}) flush log during initialization {}:{} -> {}:{}",
                            id, epoch.index(), epoch.term(), milestone.index(), milestone.term());
                    replicatedLog.flush(milestone.index(), milestone.term());
                }
                switchTo(Follower.class, restore.term, restore.ballot);
                promise.finish();
            } catch (Exception e) {
                logger.error("RaftCtx({}) initialize failed", id);
                promise.completeExceptionally(e);
                close(false);
            }
        }, true);
        return promise;
    }

    public synchronized void close(boolean gracefully) {
        if (stillRunning) {
            stillRunning = false;
        } else return;

        logger.info("RaftCtx({}) wait for replicated log convergence", id);
        if (inEventLoop()) {
            if (gracefully) quitEventLoop();
        } else if (eventLoop().isRunning()) {
            try {
                Promise promise = new Promise();
                eventLoop().enforce(() -> { if (gracefully) quitEventLoop(); promise.finish(); });
                promise.get();
            } catch (Exception e) {
                logger.error("RaftCtx({}) wait for replicated log convergence error", id, e);
            }
        } else {
            logger.error("RaftCtx({}) wait for replicated log convergence skip", id);
        }
        logger.info("RaftCtx({}) wait for replicated log convergence done", id);

        logger.info("RaftCtx({}) wait for state machine termination", id);
        try {
            if (gracefully) commandMutex.await();
        } catch (InterruptedException e) {
            logger.error("RaftCtx({}) wait for state machine termination interrupted", id);
        }
        logger.info("RaftCtx({}) wait for state machine termination done", id);

        try {
            stateMachine.close();
        } catch (Exception e) {
            logger.error("RaftCtx({}) close state machine error", id, e);
        }
        try {
            replicatedLog.close();
        } catch (Exception e) {
            logger.error("RaftCtx({}) close replicated log error", id, e);
        }
        try {
            stableStorage.close();
        } catch (Exception e) {
            logger.error("RaftCtx({}) close stable storage error", id, e);
        }
        stateMachine = null;
        replicatedLog = null;
        stableStorage = null;
        logger.info("RaftCtx({}) closed", ctxID());
    }

    public String ctxID() { return id; }
    public ID nodeID() { return cluster.localID(); }

    public RaftCluster cluster() { return cluster; }
    public RaftParticipant participant() { return ticketHolder.get().participant(); }
    public int majority() { return cluster.size() / 2 + 1; }

    public boolean inEventLoop() { return eventLoop.inEventLoop(); }
    public boolean stillRunning() { return stillRunning; }

    public RaftConfig envConfig() { return envConfig; }
    public RaftLog replicatedLog() { return replicatedLog; }
    public RaftMachine stateMachine() { return stateMachine; }
    public StableLock stableStorage() { return stableStorage; }
    public SnapshotArchive snapArchive() { return snapArchive; }
    public ContextEventLoop eventLoop() { return eventLoop; }

    /**
     * 重置定时器
     */
    public boolean resetTimer(RaftParticipant participant, boolean muted) {
        return routine.resetTimer(this, participant, muted);
    }

    /**
     * 切换角色（切换一定成功）
     * @param role 角色
     * @param term 任期
     * @param ballot 选票
     */
    public void switchTo(Class<? extends RaftParticipant> role, long term, ID ballot) {
        routine.switchTo(this, routine.trySwitch(this, role, term, ballot));
    }

    /**
     * 切换角色（切换不一定成功）
     * @param role 角色
     * @param term 任期
     * @param ballot 选票
     */
    public void trySwitchTo(Class<? extends RaftParticipant> role, long term, ID ballot) {
        if (! stillRunning) return;
        Membership membership = routine.trySwitch(this, role, term, ballot);
        if (membership != null) {
            if (inEventLoop()) {
                routine.switchTo(this, membership);
            } else {
                eventLoop().execute(() -> routine.switchTo(this, membership), true);
            }
        }
    }

    /**
     * 接收命令
     * @param currentTerm 当前任期
     * @param command 用户命令
     * @param promise 异步通知
     */
    public boolean acceptCommand(long currentTerm, Command command, Promise promise) throws Exception {
        if (! inEventLoop()) {
            throw new AssertionError("accept command should be triggered in event loop");
        }
        if (stillRunning) {
            Entry entry = replicatedLog().newEntry(currentTerm, command);
            EntryKey key = new EntryKey(entry);
            commandPromises.put(key, promise);
            promise.whenTimeout(() -> commandPromises.remove(key));
            return true;
        } else {
            promise.completeExceptionally(new ObsoleteContextException());
            return false;
        }
    }

    /**
     * 提交日志
     * @param commitIndex 提交至该索引
     * @param passiveCommit 是否被动触发提交
     */
    public void commitLog(long commitIndex, boolean passiveCommit) throws Exception {
        if (! inEventLoop()) {
            throw new AssertionError("commit log should be triggered in event loop");
        }
        if (! stillRunning) return;
        if (replicatedLog().markCommitted(commitIndex) ||
                passiveCommit && replicatedLog().lastCommitted() > stateMachine().lastApplied()) {
            // warning: remove the promise once the command is applied
            routine.commitState(this, (e) -> commandPromises.remove(new EntryKey(e)), 2);
        }
        routine.compactLog(this);
    }

    /**
     * 清空失效的通知
     */
    public void abortPromise() {
        commandPromises.clear();
    }

    /**
     * 同步快照信息：下载快照 + 应用快照
     * @param leaderId 当前 leader
     * @param lastIncludedIndex 快照中包含的最后一条日志索引
     * @param lastIncludedTerm 最后一条日志记录对应的任期
     */
    public boolean installSnapshot(ID leaderId, long lastIncludedIndex, long lastIncludedTerm) throws Exception {
        if (! inEventLoop()) {
            throw new AssertionError("install snapshot should be performed in event loop");
        }
        return stillRunning && routine.installSnapshot(this, lastIncludedIndex, lastIncludedTerm, () -> {
            SnapService snapService = cluster().remoteService(leaderId, ctxID());
            return snapService.obtainSnapshot(lastIncludedIndex, lastIncludedTerm);
        });
    }

    /**
     * 等待日志和快照之间的同步完成（若快照已经下载完毕，且状态机已经加载该快照，此时必须等待同步完成）
     */
    public void joinSnapshot() {
        PendingTask<Void> installation = snapshotInstallation;
        if (installation != null) {
            logger.warn("RaftCtx({}) join snapshot {}", id);
            try {
                installation.join();
            } catch (Exception e) {
                logger.error("RaftCtx({}) join snapshot detected exception", id, e);
            }
            routine.accomplishInstallation(this);
            logger.warn("RaftCtx({}) join snapshot completely", id);

        }
        snapArchive.cleanPending();
    }

    /**
     * 退出事件循环
     * */
    private void quitEventLoop() {
        if (! inEventLoop()) {
            throw new AssertionError("cleanup before exit should be performed in event loop");
        }
        joinSnapshot();
        abortPromise();
    }
}
