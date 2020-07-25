package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.rpc.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Candidate extends RaftMember {

    private static final Logger logger = LoggerFactory.getLogger(Candidate.class);

    private volatile boolean elected;
    private final Async.AsyncHead election = Async.head();

    public Candidate(RaftContext context, long term, ID candidate, Membership membership) {
        super(context, term, candidate, membership);
        startElection();
    }

    @Override
    public RaftResponse appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception {

        assertEventLoop();

        if (term < currentTerm) { // leader could be aware of a new election has been held
            return RaftResponse.failure(currentTerm);
        }

        logger.debug("AE-Seen[{}]({}) {} {} {} {}", leaderId, term, prevLogIndex, prevLogTerm, leaderCommit, entries);

        // term >= currentTerm means another candidate win the election in the same/later term
        ctx.switchTo(Follower.class, term, lastCandidate);
        return ctx.participant().appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public RaftResponse preVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {
        return requestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public RaftResponse requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {

        assertEventLoop();

        if (candidateId.equals(ctx.nodeID())) {
            throw new AssertionError("candidate should not invoke requestVote to itself");
        }

        logger.debug("RV-Seen[{}]({}) {} {}", candidateId, term, lastLogIndex, lastLogTerm);

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        } else if (term == currentTerm) {
            if (! candidateId.equals(lastCandidate)) {
                return RaftResponse.failure(currentTerm);
            } else if (! lastCandidate.equals(ctx.nodeID())) {
                throw new AssertionError("candidate should vote to itself");
            }
        }

        // term > currentTerm means a new election has been held, vote to the first seen candidate in new term
        ctx.switchTo(Follower.class, term, candidateId);
        return ctx.participant().requestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public void onFencing() {
        if (! elected && ! election.isAborted()) {
            election.abortRequests();
        }
    }

    @Override
    public void onTimeout() {
        ctx.switchTo(Candidate.class, currentTerm + 1, ctx.nodeID());
        RaftParticipant participant = ctx.participant();
        if (participant instanceof Candidate) {
            onFencing(); // cancel previous requestVote
        }
    }

    public void startElection() {
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
            logger.error("Start election failed {} ", ctx.ctxID(), e);
            return;
        }

        Async.AsyncHead head = election;
        if (head.isAborted()) {
            return; // fenced by other event
        }

        int majority = ctx.majority();
        long timeout = ctx.envConfig().broadcastTimeout();
        AtomicInteger votes = new AtomicInteger(1); // Vote for self
        for (ID id: ctx.cluster().remoteIDs()) {
            RaftService raftService = ctx.cluster().remoteService(id, ctx.ctxID());
            if (raftService != null) {
                try {

                    logger.debug("RequestVote[{}] {} {} {} {}", id, currentTerm, ctx.nodeID(), lastLogIndex, lastLogTerm);

                    Async<RaftResponse> response = raftService.requestVote(currentTerm, ctx.nodeID(), lastLogIndex, lastLogTerm);
                    response.on(head, timeout, (result, error, canceled) -> {
                        logger.debug("RV-Echo[{}] {} {} {} {}", id, currentTerm, result, error, canceled);
                        if (! canceled &&  error == null && result != null) {
                            if (result.term() > currentTerm) {
                                head.abortRequests();
                                ctx.trySwitchTo(Follower.class, result.term(), id);
                            } else if (result.success()) {
                                if (votes.incrementAndGet() >= majority) {
                                    elected = true;
                                    ctx.trySwitchTo(Leader.class, currentTerm, ctx.nodeID());
                                }
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.error("Invoke requestVote failed {} {}", ctx.ctxID(), id, e);
                }
            }
        }

        // if not grant enough vote
        // just wait for election timeout
    }

}
