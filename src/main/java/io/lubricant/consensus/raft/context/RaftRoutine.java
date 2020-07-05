package io.lubricant.consensus.raft.context;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.command.MaintainAgreement;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.RaftMachine.Checkpoint;
import io.lubricant.consensus.raft.context.member.*;
import io.lubricant.consensus.raft.support.PendingTask;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.SnapshotArchive;
import io.lubricant.consensus.raft.support.SnapshotArchive.PendingSnapshot;
import io.lubricant.consensus.raft.support.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.support.TimeLimited;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * 处理例程（共享的处理逻辑）
 */
public class RaftRoutine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RaftRoutine.class);

    private static final int MACHINE_IDLE = 0;
    private static final int MACHINE_HALT = -1;
    private static final int RETRY_INTERVAL = 1000; // 提交失败后的重试间隔

    private ScheduledExecutorService electionTimer; // 检查选举超时 (follower, candidate)
    private ScheduledExecutorService heartbeatKeeper; // 复制日志/发送心跳 (leader)
    private ExecutorService commandExecutor; // 提交日志/执行状态机命令
    private ExecutorService logMaintainer; // 定时生成快照/清理无效日志

    RaftRoutine() {
        electionTimer = Executors.newScheduledThreadPool(3);
        heartbeatKeeper = Executors.newScheduledThreadPool(3);
        commandExecutor = Executors.newFixedThreadPool(5);
        logMaintainer = Executors.newFixedThreadPool(5);
    }

    // 心跳超时
    private void keepAlive(RaftContext context, TimerTicket ticket) {
        long deadline = ticket.deadline().get();
        if (deadline > 0) { // if leader not change, deadline will always be Long.MAX_VALUE
            context.eventLoop().execute(() -> {
                if (resetTimer(context, ticket.participant(), false)) {
                    ticket.participant().onTimeout();
                }
            }, true);
        }
    }

    // 选举超时
    private void electionTimeout(RaftContext context, TimerTicket ticket) {
        long deadline = ticket.deadline().get();
        if (deadline > 0) { // if not timeout, deadline should be set to TimerTicket.RESET
            if (ticket.deadline().compareAndSet(deadline, TimerTicket.TIMEOUT)) {
                context.eventLoop().execute(() -> {
                    if (ticket.participant() == context.participant()) {
                        logger.info("RaftContext({}) election timeout", context.ctxID());
                        ticket.participant().onTimeout(); // may be ignore
                    }
                });
            }
        }
    }

    /**
     * 重置定时器
     * @param context 上下文
     * @param participant 参与者
     * @param muted 暂停计时
     * @return 重置定时器是否成功（当前参与者已经失效/被替换）
     */
    public boolean resetTimer(RaftContext context, RaftParticipant participant, boolean muted) {
        if (! context.inEventLoop()) {
            throw new AssertionError("reset timer should be performed in event loop");
        }

        AtomicReference<TimerTicket> ticketHolder = context.ticketHolder;

        TimerTicket exist = ticketHolder.get();
        long moment = exist == null ? 0: exist.deadline().get(); // the same object instance of participant
        if (exist != null && exist.participant() != participant || moment < 0)
            return false;

        long now = System.currentTimeMillis();
        long timeout = (participant instanceof Leader) ?
                context.envConfig().heartbeatInterval():
                muted ? Long.MAX_VALUE : context.envConfig().electionTimeout();

        long deadline = Math.max( Math.min(moment, Long.MAX_VALUE - 1) + 1, now + timeout);
        boolean reset = exist == null ||
                        exist.deadline().compareAndSet(moment, TimerTicket.INVALID) &&
                        exist.schedule().cancel(true);

        if (participant instanceof Leader || reset) { // reset == true means not timeout yet
            TimerTicket ticket;
            ScheduledFuture<?> schedule;
            if (participant instanceof Leader) {
                ticket = new TimerTicket(participant, Long.MAX_VALUE);
                schedule = heartbeatKeeper.schedule(() -> keepAlive(context, ticket), exist == null ? 0: timeout, TimeUnit.MILLISECONDS);
            } else {
                ticket = new TimerTicket(participant, deadline);
                schedule = electionTimer.schedule(() -> electionTimeout(context, ticket), deadline - now, TimeUnit.MILLISECONDS);
            }
            if (! ticketHolder.compareAndSet(exist, ticket.with(schedule))) {
                throw new AssertionError("concurrent modification is not allowed");
            }
            return true;
        }

        return false;
    }

    /**
     * 尝试切换角色
     * @param context 上下文
     * @param role 角色
     * @param term 任期
     * @param ballot 选票
     * @return 将要切换到的角色
     */
    public Membership trySwitch(RaftContext context, Class<? extends RaftParticipant> role, long term, ID ballot) {
        AtomicReference<Membership> membershipFilter = context.membershipFilter;
        Membership next = new Membership(role, term, ballot);
        Membership current = membershipFilter.get();
        while (next.isBetter(current)) {
            if (membershipFilter.compareAndSet(current, next)) {
                return next;
            } else {
                current = membershipFilter.get();
            }
        }
        return null;
    }

    /**
     * 切换角色
     * @param context 上下文
     * @param expected 期望切换的角色
     */
    public void switchTo(RaftContext context, Membership expected) {
        if (! context.inEventLoop()) {
            throw new AssertionError("role switch should be performed in event loop");
        }

        AtomicReference<Membership> memberFilter = context.membershipFilter;
        Membership current = memberFilter.get();
        if (expected == null) {
            expected = current; // trySwitch false, just apply the latest membership
        }

        if (expected.isBetter(current)) {
            throw new AssertionError("no downgrade conversion allowed");
        }

        while (current.notApplied()) {
            if (memberFilter.compareAndSet(current, new Membership(current))) {
                convertTo(context, current);
            } else {
                current = memberFilter.get();
            }
        }
    }

    private void convertTo(RaftContext context, Membership member) {

        AtomicReference<TimerTicket> ticketHolder = context.ticketHolder;
        TimerTicket exist = ticketHolder.get();
        if (exist != null) {
            if (exist.term() > member.term()) {
                // trying to replace newer ticket with expired one, just ignore
                return;
            }
            long deadline = exist.deadline().get();
            if (deadline > 0) {
                if (exist.deadline().compareAndSet(deadline, TimerTicket.FENCING)) {
                    exist.participant().onFencing();
                }
            }
            ticketHolder.set(null);
        }

        RaftParticipant participant;
        try {
            participant = member.role().
                    getConstructor(RaftContext.class, long.class, ID.class, Membership.class).
                    newInstance(context, member.term(), member.ballot(), member);
        } catch (Exception e) {
            throw new Error(e);
        }

        logger.info("RaftContext({}) convert to {}({})", context.ctxID(),
                member.role().getSimpleName(), member.term());

        if (! resetTimer(context, participant, false)) {
            throw new AssertionError("initial reset timer must complete successfully");
        }
    }

    /**
     * 提交日志
     * @param context 上下文
     * @param promises 异步回调通知池
     * @param maxRounds 单次触发最多可执行次数
     */
    public void commitState(RaftContext context, Function<Entry, Promise> promises, int maxRounds) {
        AtomicInteger version = context.commitVersion;
        int v = version.getAndUpdate(x -> (x == Integer.MAX_VALUE || x == MACHINE_HALT) ? x : x + 1);
        if (v == 0) {
            commandExecutor.execute(() -> applyEntry(context, promises, maxRounds));
        } else if (v < 0) {
            throw new AssertionError("commitment is not allow during recovery from checkpoint");
        }
    }

    private void applyEntry(RaftContext context, Function<Entry, Promise> promises, int maxRounds) {
        AtomicInteger version = context.commitVersion;
        int t = maxRounds - 1;
        int v = version.get();
        while (v > 0) {
            if (applyCommand(context, promises, v)) {
                if ((v = version.addAndGet(-v)) > 0) { // check whether to proceed to the next round
                    if (--t < 0) { // yield the thread in fairness
                        commandExecutor.execute(() -> applyEntry(context, promises, maxRounds));
                        break;
                    }
                }
            } else {
                TimeLimited.newTimer(RETRY_INTERVAL).schedule(() ->  // retry later
                        commandExecutor.execute(() -> applyEntry(context, promises, maxRounds)));
            }
        }
    }

    private boolean applyCommand(RaftContext context, Function<Entry, Promise> promises, int count){
        try {
            RaftLog log = context.replicatedLog();
            RaftMachine machine = context.stateMachine();
            final long commitIndex = log.lastCommitted();
            long prevApplied = machine.lastApplied();
            while (prevApplied < commitIndex && count-- > 0) {
                long nextIndex = machine.lastApplied() + 1;
                Entry entry = log.get(nextIndex);
                if (entry == null) {
                    throw new AssertionError("no vacancies allowed in log");
                }
                Promise promise = promises.apply(entry);
                try {
                    Object result = machine.apply(entry);
                    if (promise != null)
                        promise.complete(result);
                } catch (Throwable ex) {
                    if (promise != null)
                        promise.completeExceptionally(ex);
                }
                long lastApplied = machine.lastApplied();
                if (lastApplied <= prevApplied) {
                    logger.error("RaftContext({}) application stuck at {}", context.ctxID(), lastApplied);
                    return false; // may be something wrong, retry latter
                }
                prevApplied = lastApplied;
                context.maintainAgreement.increaseCommand();
            }
        } catch (Exception e) {
            logger.error("RaftContext({}) apply command failed", context.ctxID(), e);
            return false;
        } finally {
            maintainSnap(context);
        }

        return true;
    }

    private void maintainSnap(RaftContext context) {
        MaintainAgreement agreement = context.maintainAgreement;
        RaftMachine machine = context.stateMachine();
        if (agreement.needMaintain(machine.lastApplied())) {
            agreement.triggerMaintenance();
            logger.info("RaftContext({}) maintain triggered", context.ctxID());
            try {
                Future<Checkpoint> future = machine.checkpoint(agreement.minimalLogIndex());
                logMaintainer.execute(() -> {
                    boolean maintainSuccess = false;
                    boolean updateLastInclude = false;
                    SnapshotArchive archive = context.snapArchive();
                    try {
                        if (future != null) {
                            RaftLog log = context.replicatedLog();
                            Checkpoint checkpoint = future.get();
                            if (checkpoint != null) {
                                Entry entry = log.get(checkpoint.lastIncludeIndex());
                                if (entry == null) {
                                    Entry epoch = log.epoch();
                                    throw new AssertionError(String.format("checkpoint index not found %d (%d) (%d-%d)",
                                            checkpoint.lastIncludeIndex(), agreement.minimalLogIndex(), epoch.index(), epoch.term()));
                                }
                                if (updateLastInclude = archive.takeSnapshot(checkpoint, entry.term())) {
                                    agreement.snapshotIncludeEntry(entry.index(), entry.term());
                                }
                            }
                        }
                        maintainSuccess= true;
                    } catch (Exception e) {
                        logger.error("RaftContext({}) record checkpoint failed", context.ctxID(), e);
                    }

                    if (! updateLastInclude) {
                        try {
                            Snapshot snapshot = archive.lastSnapshot();
                            if (snapshot != null) {
                                agreement.snapshotIncludeEntry(snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm());
                            }
                        } catch (Exception e) {
                            logger.error("RaftContext({}) update lastIncludedEntry failed", context.ctxID(), e);
                        }
                    }
                    agreement.finishMaintenance(maintainSuccess);
                    logger.info("RaftContext({}) maintain finished: {}", context.ctxID(), maintainSuccess);
                });
            } catch (Exception e) {
                logger.error("RaftContext({}) record checkpoint failed", context.ctxID(), e);
                agreement.finishMaintenance(false);
            }
        }
    }

    /**
     * 整理日志
     * @param context 上下文
     */
    public void compactLog(RaftContext context) {
        MaintainAgreement agreement = context.maintainAgreement;
        if (agreement.needCompact()) {
            agreement.triggerCompaction();
            logger.info("RaftContext({}) compact triggered", context.ctxID());
            try {
                Entry snap = agreement.snapshotIncludeEntry();
                RaftLog log = context.replicatedLog();
                Entry entry = log.get(snap.index());
                if (entry != null) {
                    if (entry.term() != snap.term()) {
                        throw new AssertionError(String.format(
                                "committed log entry (%d-%d) mismatch whit snapshot %d-%d",
                                entry.index(), entry.term(), snap.index(), snap.term()));
                    }
                    Future<Boolean> future = log.flush(entry.index());
                    logMaintainer.execute(() -> {
                        boolean compactSuccess = false;
                        try {
                            if (future != null && Boolean.TRUE.equals(future.get())) {
                                agreement.minimalLogIndex(log.epoch().index());
                                compactSuccess = true;
                            }
                        } catch (Exception e) {
                            logger.error("RaftContext({}) compact log failed", context.ctxID(), e);
                        }
                        agreement.finishCompaction(compactSuccess);
                        logger.info("RaftContext({}) compact finished: {}", context.ctxID(), compactSuccess);
                    });
                }
            } catch (Exception e) {
                logger.error("RaftContext({}) compact log failed", context.ctxID(), e);
                agreement.finishCompaction(false);
            }
        }
    }

    /**
     * 应用快照
     * @param context 上下文
     * @param lastIncludedIndex 快照结尾日志索引
     * @param lastIncludedTerm 结尾记录对应的任期
     */
    public boolean installSnapshot(RaftContext context, long lastIncludedIndex, long lastIncludedTerm, Supplier<PendingTask<Snapshot>> snapSupplier) throws Exception {
        SnapshotArchive snapArchive = context.snapArchive();
        PendingTask<Void> installation = context.snapshotInstallation;
        if (installation == null) {
            PendingSnapshot snapshot = snapArchive.pendSnapshot(lastIncludedIndex, lastIncludedTerm, null);
            if (snapshot == null) {
                snapshot = snapArchive.pendSnapshot(lastIncludedIndex, lastIncludedTerm, snapSupplier.get());
            }
            if (snapshot.isSuccess()) {
                context.snapshotInstallation = restoreCheckpoint(context, snapshot.snapshot());
            } else if (! snapshot.isPending()) { // download snapshot fail
                snapArchive.cleanPending(); // retry later
            }
        } else if (! installation.isPending()) {
            if (installation.isSuccess()) {
                snapArchive.cleanPending();
                context.snapshotInstallation = null;
                return true;
            }
            PendingSnapshot snapshot = snapArchive.pendSnapshot(lastIncludedIndex, lastIncludedTerm, null);
            if (snapshot.isExpired(lastIncludedIndex, lastIncludedTerm)) { // snapshot is expired
                snapArchive.pendSnapshot(lastIncludedIndex, lastIncludedTerm, snapSupplier.get()); // update snapshot
                context.snapshotInstallation = null;
            } else { // apply snapshot fail
                context.snapshotInstallation = restoreCheckpoint(context, snapshot.snapshot()); // try apply again
            }
        }
        return false;
    }

    /**
     * 重置状态
     * @param context 上下文
     * @param snapshot 状态快照
     */
    private PendingTask<Void> restoreCheckpoint(RaftContext context, Snapshot snapshot) throws Exception {

        final RaftLog log = context.replicatedLog();
        Entry last = log.last();
        if (last == null) {
            last = log.epoch();
        }

        if (snapshot.lastIncludeIndex() < last.index() ||
            snapshot.lastIncludeIndex() == last.index() && snapshot.lastIncludeTerm() < last.term()) {
            throw new AssertionError(String.format( "illegal snapshot (%d:%d) < (%d:%d)",
                    snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm(), last.index(), last.term()));
        }

        AtomicInteger version = context.commitVersion;
        if (version.compareAndSet(MACHINE_IDLE, MACHINE_HALT)) {
            PendingTask<Void> pendingCheckpoint = new PendingTask<Void>(){
                @Override
                protected void perform() {
                    try {
                        RaftMachine machine = context.stateMachine();
                        final long commitIndex = log.lastCommitted();
                        if (snapshot.lastIncludeIndex() <= commitIndex) {
                            throw new AssertionError(String.format(
                                    "checkpoint index is included %d <= %d", snapshot.lastIncludeIndex(), commitIndex));
                        }
                        machine.recover(snapshot);
                        set(null);
                    } catch (Exception e) {
                        logger.error("RaftContext({}) restore from snapshot failed", context.ctxID(), e);
                        setException(e);
                    }
                }
            };
            commandExecutor.execute(() -> {
                if (version.get() == MACHINE_HALT) {
                    pendingCheckpoint.perform(null);
                    if (version.compareAndSet(MACHINE_HALT, MACHINE_IDLE)) {
                        return;
                    }
                }
                throw new AssertionError("illegal commit version " + version.get());
            });
            return pendingCheckpoint;
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        electionTimer.shutdown();
        heartbeatKeeper.shutdown();
        commandExecutor.shutdown();
        logMaintainer.shutdown();
        try {
            electionTimer.awaitTermination(5, TimeUnit.SECONDS);
            heartbeatKeeper.awaitTermination(5, TimeUnit.SECONDS);
            commandExecutor.awaitTermination(5, TimeUnit.SECONDS);
            logMaintainer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted during await termination", e);
        }
    }
}
