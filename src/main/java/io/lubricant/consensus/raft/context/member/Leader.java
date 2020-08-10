package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.anomaly.NotLeaderException;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.rpc.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Leader extends RaftMember implements Leadership {

    private static final Logger logger = LoggerFactory.getLogger(Leader.class);

    private final Async.AsyncHead replication = Async.head();
    private Map<ID, State> followerStatus;

    public Leader(RaftContext context, long term, ID candidate, Membership membership) {
        super(context, term, candidate, membership);
        ctx.abortPromise();
        onTimeout();
    }

    private void prepareReplication() throws Exception {
        if (followerStatus != null) return;
        Entry last = ctx.replicatedLog().last();
        Entry epoch = ctx.replicatedLog().epoch();
        long lastIndex = last == null ? epoch.index(): last.index();
        Set<ID> followers = ctx.cluster().remoteIDs();
        Map<ID, State> map = new HashMap<>(followers.size());
        for (ID follower : followers) {
            State state = new State();
            state.lastEpoch = epoch.index();
            state.nextIndex = lastIndex + 1;
            map.put(follower, state);
        }
        final List<State> states = Arrays.asList(map.values().toArray(new State[0]));
        followerStatus = Collections.unmodifiableMap(new ConcurrentHashMap<ID, State>(map) {
            @Override
            public Collection<State> values() {
                return states;
            }
        });
    }

    public boolean isReady() {
        if (followerStatus != null) {
            long now = System.currentTimeMillis();
            int ready = 1, half = followerStatus.size() / 2;
            for (State state : followerStatus.values()) {
                if (state.isReady(
                        ctx.envConfig().availableCriticalPoint(),
                        ctx.envConfig().recoveryCoolDownMills(), now) && ++ready > half)
                    return true;
            }
        }
        return false;
    }

    @Override
    public RaftResponse appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception {

        assertEventLoop();

        if (leaderId.equals(ctx.nodeID())) {
            throw new AssertionError("leader should not invoke appendEntries to itself");
        }

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        }

        if (term == currentTerm) {
            throw new AssertionError("there can be only one leader in the same term");
        }

        // a new leader has been elected
        ctx.switchTo(Follower.class, currentTerm, lastCandidate);
        return ctx.participant().appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public RaftResponse preVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {
        return RaftResponse.failure(currentTerm); // refuse grant vote event term greater than currentTerm
    }

    @Override
    public RaftResponse requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {

        assertEventLoop();

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        } else if (term == currentTerm) {
            if (lastCandidate.equals(ctx.nodeID())) {
                return RaftResponse.failure(currentTerm);
            } else {
                throw new AssertionError("leader should vote to itself");
            }
        }

        // a new election has been held, vote to the first seen candidate in new term
        ctx.switchTo(Follower.class, currentTerm, candidateId);
        return ctx.participant().requestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public void onFencing() {
        replication.abortRequests();
        ctx.abortPromise();
    }

    @Override
    public void onTimeout() {
        try {
            replicateLog(true);
        } catch (Exception e) {
            logger.error("RaftCtx({}) send heartbeat failed", ctx.ctxID(), e);
        }
    }

    public void acceptCommand(Command command, Promise promise) {
        try {
            if (replication.isAborted()) {
                promise.completeExceptionally(new NotLeaderException(ctx.participant()));
                return;
            }
            if (ctx.acceptCommand(currentTerm, command, promise)) {
                replicateLog(false);
            }
        } catch (Exception e) {
            logger.error("RaftCtx({}) accept command failed", ctx.ctxID(), e);
        }
    }

    private void replicateLog(boolean heartbeat) throws Exception {

        assertEventLoop();
        prepareReplication();

        Async.AsyncHead head = replication;
        if (head.isAborted()) {
            return; // fenced by other event
        }

        final Entry epoch = ctx.replicatedLog().epoch();
        final long leaderCommit = ctx.replicatedLog().lastCommitted();
        final long timeout = ctx.envConfig().broadcastTimeout();
        final long now = System.currentTimeMillis();
        for (ID id: ctx.cluster().remoteIDs()) {
            State state = followerStatus.get(id);
            state.increaseMono(lastRequest, state.lastRequest, now);
            RaftService raftService = ctx.cluster().remoteService(id, ctx.ctxID());
            if (raftService != null) {
                try {
                    int requestLimit = IN_FLIGHT_LIMIT / (heartbeat ? 10: 1);
                    if (state.requestInFlight > requestLimit) {
                        logger.debug("ReplicateLog[{}] {} {} @{}", id, currentTerm, ctx.nodeID(), state.requestInFlight);
                        continue;
                    }

                    if (state.pendingInstallation) {

                        logger.debug("InstallSnapshot[{}] {} {} {} {}", id, currentTerm, ctx.nodeID(), epoch.index(), epoch.term());

                        Async<RaftResponse> response = raftService.installSnapshot(currentTerm, ctx.nodeID(), epoch.index(), epoch.term());
                        requestInFlight.incrementAndGet(state);
                        response.on(head, timeout, (result, error, canceled) -> {
                            logger.debug("IS-Echo[{}] {} {} {} <{}:{}>", id, currentTerm, result, error, canceled, epoch.index(), epoch.term());
                            requestInFlight.decrementAndGet(state);
                            if (! canceled && error == null && result != null) {
                                if (result.term() > currentTerm) {
                                    head.abortRequests();
                                    ctx.trySwitchTo(Follower.class, result.term(), id);
                                } else {
                                    state.statSuccess(System.currentTimeMillis(), ! result.success());
                                    state.updateIndex(epoch.index(), epoch.index(), result.success(), true);
                                }
                            } else {
                                state.statFailure(System.currentTimeMillis(), error != null, result != null && ! result.success());
                            }
                        });
                        continue; // wait for the installation to complete ...
                    }

                    long prevTerm = epoch.term(), prevIndex = epoch.index(), lastIndex;
                    long nextIndex = Math.max(state.nextIndex - 1, epoch.index());
                    int fetchLimit = REPLICATE_LIMIT >> (heartbeat ? 1 : 0);
                    Entry[] entries = ctx.replicatedLog().batch(nextIndex, fetchLimit + 1);
                    if (entries != null && entries.length > 0) {
                        Entry prevEntry = entries[0];
                        if (prevEntry.index() == nextIndex) {
                            prevTerm = prevEntry.term();
                            prevIndex = prevEntry.index();
                            entries = Arrays.copyOfRange(entries, 1, entries.length);
                        } else if (prevEntry.index() != epoch.index() + 1) {
                            throw new AssertionError("log index should start with epoch.index + 1");
                        }
                        if (entries.length == 0) {
                            lastIndex = prevIndex;
                        } else {
                            lastIndex = entries[entries.length - 1].index();
                        }
                    } else {
                        lastIndex = epoch.index();
                    }

                    logger.debug("AppendEntries[{}] {} {} {} {} {} {}", id, currentTerm, ctx.nodeID(), prevIndex, prevTerm, entries, leaderCommit);

                    Async<RaftResponse> response = raftService.appendEntries(currentTerm, ctx.nodeID(), prevIndex, prevTerm, entries, leaderCommit);
                    requestInFlight.incrementAndGet(state);
                    response.on(head, timeout, (result, error, canceled) -> {
                        // logger.debug("{} {} {} {} {}", id, state.lastEpoch, state.nextIndex, state.matchIndex, state.pendingInstallation);
                        logger.debug("AE-Echo[{}] ({}/{}) {} {} {} <{}:{}>", id, nextIndex, lastIndex, result, error, canceled, epoch.index(), epoch.term());
                        requestInFlight.decrementAndGet(state);
                        final long current = System.currentTimeMillis();
                        if (! canceled && error == null && result != null) {
                            if (result.term() > currentTerm) {
                                head.abortRequests();
                                ctx.trySwitchTo(Follower.class, result.term(), id);
                            } else {
                                state.statSuccess(current, ! result.success());
                                state.updateIndex(epoch.index(), lastIndex, result.success(), false);
                                if (result.success()) {
                                    tryCommit();
                                }
                            }
                        } else {
                            state.statFailure(current, error != null, result != null && ! result.success());
                        }
                    });
                } catch (Exception e) {
                    logger.error("RaftCtx({}) invoke appendEntries failed {}", ctx.ctxID(), id, e);
                }
            } else {
                state.statFailure(now, true, false); // service is not available
            }
        }
    }

    private void tryCommit()  {
        long[] majorIndices = State.majorIndices(followerStatus.values());
        long fullIndex = majorIndices[0]; // replicated to all nodes
        long majorIndex = majorIndices[1]; // replicated to major nodes
        if (fullIndex > majorIndex) {
            throw new AssertionError("impossible replication status");
        }
        if (majorIndex != 0) try {
            long commitIndex;
            Entry major = ctx.replicatedLog().get(majorIndex);
            if (major.term() == currentTerm) {
                commitIndex = majorIndex; // commit the entry from current leader which replicated to major nodes
            } else {
                commitIndex = fullIndex; // commit the entry from previous leader which replicated to all nodes
            }
            if (commitIndex != 0 && commitIndex != ctx.replicatedLog().lastCommitted()) {
                if (ctx.eventLoop().isAvailable()) {
                    if (ctx.inEventLoop()) {
                        ctx.commitLog(commitIndex, false);
                    } else {
                        ctx.eventLoop().execute(() -> {
                            try {
                                ctx.commitLog(commitIndex, false);
                            } catch (Exception e) {
                                logger.error("RaftCtx({}) commit log failed", ctx.ctxID(), e);
                            }
                        });
                    }
                }
            }
        } catch (Exception e) {
            logger.error("RaftCtx({}) try commit failed", ctx.ctxID(), e);
        }
    }

}
