package io.lubricant.consensus.raft.context;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.context.member.*;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.TimeLimited;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


/**
 * 处理例程（共享的处理逻辑）
 */
public class RaftRoutine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RaftRoutine.class);

    private static final int RETRY_INTERVAL = 1000; // 提交失败后的重试间隔

    private ScheduledExecutorService electionTimer; // 检查选举超时 (follower, candidate)
    private ScheduledExecutorService heartbeatKeeper; // 复制日志/发送心跳 (leader)
    private ExecutorService commandExecutor; // 提交日志/执行状态机命令

    RaftRoutine() {
        electionTimer = Executors.newScheduledThreadPool(3);
        heartbeatKeeper = Executors.newScheduledThreadPool(3);
        commandExecutor = Executors.newFixedThreadPool(5);
    }

    // 心跳超时
    private void keepAlive(RaftContext context, TimerTicket ticket) {
        long deadline = ticket.deadline().get();
        if (deadline > 0) { // if leader not change, deadline will always be Long.MAX_VALUE
            context.eventLoop().execute(() -> {
                if (resetTimer(context, ticket.participant())) {
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
     * @return 重置定时器是否成功（当前参与者已经失效/被替换）
     */
    public boolean resetTimer(RaftContext context, RaftParticipant participant) {
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
                context.envConfig().electionTimeout();

        long deadline = Math.max( Math.min(moment, Long.MAX_VALUE - 1) + 1, now + timeout);
        boolean reset = exist == null ||
                        exist.deadline().compareAndSet(moment, TimerTicket.RESET) &&
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

        if (! resetTimer(context, participant)) {
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
        int v = version.getAndUpdate(x -> (x == Integer.MAX_VALUE) ? x : x + 1);
        if (v == 0) {
            commandExecutor.execute(() -> applyEntry(context, promises, maxRounds));
        }
    }

    private void applyEntry(RaftContext context, Function<Entry, Promise> promises, int maxRounds) {
        AtomicInteger version = context.commitVersion;
        int t = maxRounds - 1;
        int v = version.get();
        while (v > 0) {
            if (applyCommand(context, promises)) {
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

    private boolean applyCommand(RaftContext context, Function<Entry, Promise> promises){
        try {
            RaftLog log = context.replicatedLog();
            RaftMachine machine = context.stateMachine();
            final long commitIndex = log.lastCommitted();
            long prevApplied = machine.lastApplied();
            while (prevApplied < commitIndex) {
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
                } catch (Exception ex) {
                    if (promise != null)
                        promise.error(ex);
                }
                long lastApplied = machine.lastApplied();
                if (lastApplied <= prevApplied) {
                    logger.error("RaftContext({}) application stuck at {}", context.ctxID(), lastApplied);
                    return false; // may be something wrong, retry latter
                }
                prevApplied = lastApplied;
            }
        } catch (Exception e) {
            logger.error("Apply command failed {}", context.ctxID(), e);
            return false;
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        electionTimer.shutdown();
        heartbeatKeeper.shutdown();
        commandExecutor.shutdown();
        try {
            electionTimer.awaitTermination(5, TimeUnit.SECONDS);
            heartbeatKeeper.awaitTermination(5, TimeUnit.SECONDS);
            commandExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted during await termination", e);
        }
    }
}
