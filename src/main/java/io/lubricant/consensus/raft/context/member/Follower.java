package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.rpc.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class Follower extends RaftMember {

    private static final Logger logger = LoggerFactory.getLogger(Follower.class);

    private ID currentLeader; // 已知的最新 leader（用于重定向）

    private boolean timeoutDetected = false;
    private final Async.AsyncHead qualifier = Async.head();

    public Follower(RaftContext context, long term, ID candidate, Membership membership) {
        super(context, term, candidate, membership);
    }

    public ID currentLeader() {
        return currentLeader;
    }

    @Override
    public RaftResponse appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception {

        assertEventLoop();

        if (term < currentTerm) { // leader could be aware of a new election has been held
            return RaftResponse.failure(currentTerm);
        }

        ctx.resetTimer(this, true);

        if (term > currentTerm || timeoutDetected) {
            ctx.switchTo(Follower.class, term, lastCandidate);
            return ctx.participant().appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        } else if (currentLeader != null && ! leaderId.equals(currentLeader)) {
            throw new AssertionError("only one leader could be elected in one term");
        }

        ctx.joinSnapshot(); // ensure consistent between log epoch and machine status

        currentLeader = leaderId;
        try {

            if (! logContains(prevLogIndex, prevLogTerm)) {
                return RaftResponse.failure(currentTerm);
            }

            // skip entries before log epoch
            entries = purgeEntries(entries);

            logger.debug("AE-Seen[{}]({}) {} {} {} {}", leaderId, term, prevLogIndex, prevLogTerm, leaderCommit, entries);

            RaftLog log = ctx.replicatedLog();
            // leader may send heartbeat without log entries
            if (entries != null && entries.length > 0) {
                Entry conflict = log.conflict(entries);
                if (conflict != null) {
                    log.truncate(conflict.index());
                }
                log.append(entries); // sync to disk
            }

            if (leaderCommit > log.epoch().index()) {
                Entry last = log.last();
                if (last != null) {
                    // truncate commit index when it exceed the last index
                    ctx.commitLog(Math.min(leaderCommit, last.index()), true);
                }
            }
        } finally {
            ctx.resetTimer(this, false);
        }

        return RaftResponse.success(term);
    }

    @Override
    public RaftResponse preVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {
        assertEventLoop();

        if (term <= currentTerm || ! timeoutDetected) {
            return RaftResponse.failure(currentTerm);
        }

        ctx.resetTimer(this, true);
        try {
            return RaftResponse.reply(currentTerm,
                    logUpToDate(lastLogIndex, lastLogTerm));
        } finally {
            ctx.resetTimer(this, false);
        }
    }

    @Override
    public RaftResponse requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {

        assertEventLoop();

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        } else if (term == currentTerm) {
            return RaftResponse.reply(currentTerm, candidateId.equals(votedFor()));
        }

        ctx.resetTimer(this, true);
        ctx.joinSnapshot(); // ensure consistent between log epoch and machine status

        // term > currentTerm means a new election has been held, vote for the first seen candidate
        boolean voteGranted = logUpToDate(lastLogIndex, lastLogTerm);
        ID voteFor = voteGranted ? candidateId : null; // refuse to vote

        ctx.switchTo(Follower.class, term, voteFor);
        return ctx.participant().requestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public RaftResponse installSnapshot(long term, ID leaderId, long lastIncludedIndex, long lastIncludedTerm) throws Exception {

        assertEventLoop();

        ctx.resetTimer(this, true);

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        } else if (term > currentTerm) {
            throw new AssertionError("leader invoke InstallSnapshot before AppendEntries");
        } else if (timeoutDetected) {
            ctx.switchTo(Follower.class, currentTerm, lastCandidate);
            return ctx.participant().installSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm);
        }

        logger.debug("IS-Seen[{}]({}) {} {}", leaderId, term, lastIncludedIndex, lastIncludedTerm);

        try {
            boolean success = ctx.installSnapshot(leaderId, lastIncludedIndex, lastIncludedTerm);
            return RaftResponse.reply(currentTerm, success);
        } finally {
            ctx.resetTimer(this, false);
        }
    }

    @Override
    public void onTimeout() {
        ctx.joinSnapshot();
        if (ctx.envConfig().preVote()) {
            ctx.switchTo(Follower.class, currentTerm, ctx.nodeID());
            RaftParticipant participant = ctx.participant();
            if (participant instanceof Follower && participant.currentTerm() == currentTerm) {
                ((Follower) participant).prepareElection();
                onFencing();
            }
        } else {
            ctx.switchTo(Candidate.class, currentTerm + 1, ctx.nodeID());
        }
    }

    @Override
    public void onFencing() {
        assertEventLoop();
        ctx.joinSnapshot();
        qualifier.abortRequests();
    }

    private boolean logContains(long index, long term) throws Exception {
        if (index == 0 && term == 0) return true;
        if (index == 0 || term == 0) {
            throw new AssertionError("index and term should be 0 at the same time");
        }
        Entry epoch = ctx.replicatedLog().epoch();
        if (index <= epoch.index()) {
            if (index == epoch.index() && term != epoch.term()) {
                throw new AssertionError("committed index and term should be exactly the same");
            }
            return true;
        }
        Entry entry = ctx.replicatedLog().get(index);
        return entry != null && entry.term() == term;
    }

    private boolean logUpToDate(long index, long term) throws Exception {
        Entry last = ctx.replicatedLog().last();
        if (last != null) {
            return term > last.term() || (term == last.term() && index >= last.index());
        } else {
            Entry epoch = ctx.replicatedLog().epoch();
            if (index > epoch.index() && term < epoch.term() ||
                index == epoch.index() && term != epoch.term()) {
                throw new AssertionError(String.format(
                        "impossible log status: follower-epoch(%d:%d) candidate-last(%d:%d)",
                        epoch.index(), epoch.term(), index, term));
            }
            return index >= epoch.index();
        }
    }

    private Entry[] purgeEntries(Entry[] entries) throws Exception {
        long epochIndex = ctx.replicatedLog().epoch().index();
        if (entries != null && entries.length > 0 && entries[0].index() <= epochIndex) {
            int cursor = 0;
            while (entries[cursor].index() <= epochIndex && ++cursor < entries.length);
            if (cursor == entries.length) {
                entries = null;
            } else {
                entries = Arrays.copyOfRange(entries, cursor, entries.length);
            }
        }
        return entries;
    }

    private void prepareElection() {

        timeoutDetected = true;

        long lastLogIndex;
        long lastLogTerm;
        try {
            Entry last = ctx.replicatedLog().last();
            if (last == null) {
                last = ctx.replicatedLog().epoch();
            }
            lastLogIndex = last.index();
            lastLogTerm = last.term();
        } catch (Exception e) {
            logger.error("Start pre-vote failed {} ", ctx.ctxID(), e);
            return;
        }

        Async.AsyncHead head = qualifier;
        if (head.isAborted()) {
            return; // fenced by other event
        }

        final long nextTerm = currentTerm + 1;
        int majority = ctx.majority();
        long timeout = ctx.envConfig().broadcastTimeout();
        AtomicInteger votes = new AtomicInteger(1); // PreVote for self
        for (ID id: ctx.cluster().remoteIDs()) {
            RaftService raftService = ctx.cluster().remoteService(id, ctx.ctxID());
            if (raftService != null) {
                try {

                    logger.debug("PreVote[{}] {} {} {} {}", id, nextTerm, ctx.nodeID(), lastLogIndex, lastLogTerm);

                    Async<RaftResponse> response = raftService.preVote(nextTerm, ctx.nodeID(), lastLogIndex, lastLogTerm);
                    response.on(head, timeout, (result, error, canceled) -> {
                        logger.debug("PV-Echo[{}] {} {} {} {}", id, nextTerm, result, error, canceled);
                        if (! canceled &&  error == null && result != null) {
                            if (result.term() > nextTerm) {
                                head.abortRequests();
                                ctx.trySwitchTo(Follower.class, result.term(), id);
                            } else if (result.success()) {
                                if (votes.incrementAndGet() >= majority) {
                                    ctx.trySwitchTo(Candidate.class, nextTerm, ctx.nodeID());
                                }
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.error("Invoke preVote failed {} {}", ctx.ctxID(), id, e);
                }
            }
        }

        // if not grant enough vote
        // just wait for election timeout
    }
}
