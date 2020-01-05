package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Follower extends RaftMember {

    private static final Logger logger = LoggerFactory.getLogger(Follower.class);

    private ID currentLeader; // 已知的最新 leader（用于重定向）

    public Follower(RaftContext context, long term, ID candidate, Membership membership) {
        super(context, term, candidate, membership);
    }

    public ID currentLeader() {
        return currentLeader;
    }

    @Override
    public RaftResponse appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception {

        assertEventLoop();

        if (term < currentTerm) { // leader could aware of a new election has benn held
            return RaftResponse.failure(currentTerm);
        }

        if (term > currentTerm) {
            ctx.switchTo(Follower.class, term, lastCandidate);
            return ctx.participant().appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        } else if (currentLeader != null && ! leaderId.equals(currentLeader)) {
            throw new AssertionError("only one leader could be elected in one term");
        }

        currentLeader = leaderId;
        ctx.resetTimer(this);

        if (! logContains(prevLogIndex, prevLogTerm)) {
            return RaftResponse.failure(currentTerm);
        }

        logger.debug("Request[{}]({}) {} {} {} {}", leaderId, term, prevLogIndex, prevLogTerm, leaderCommit, entries);

        // leader may send heartbeat without log entries
        if (entries != null && entries.length > 0) {
            RaftLog log = ctx.replicatedLog();

            Entry conflict = log.conflict(entries);
            if (conflict != null) {
                log.truncate(conflict.index());
            }
            log.append(entries); // sync to disk

            Entry last = log.last();
            if (leaderCommit != 0 && last != null) {
                // truncate commit index when it exceed the last index
                ctx.commitLog(Math.min(leaderCommit, last.index()), true);
            }
        }

        ctx.resetTimer(this);
        return RaftResponse.success(term);
    }

    @Override
    public RaftResponse requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {

        assertEventLoop();

        if (term < currentTerm) {
            return RaftResponse.failure(currentTerm);
        } else if (term == currentTerm) {
            return RaftResponse.reply(currentTerm, candidateId.equals(votedFor()));
        }

        // term > currentTerm means a new election has been held, vote for the first seen candidate
        boolean voteGranted = logUpToDate(lastLogIndex, lastLogTerm);
        ID voteFor = voteGranted ? candidateId : null; // refuse to vote

        ctx.switchTo(Follower.class, term, voteFor);
        return ctx.participant().requestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public void onTimeout() {
        ctx.switchTo(Candidate.class, currentTerm + 1, ctx.nodeID());
        RaftParticipant participant = ctx.participant();
        if (participant instanceof Candidate) {
            ((Candidate) participant).startElection();
        }
    }

    private boolean logContains(long index, long term) throws Exception {
        if (index == 0 && term == 0) return true;
        if (index == 0 || term == 0) {
            throw new AssertionError("index and term only be 0 at the same time");
        }
        Entry entry = ctx.replicatedLog().get(index);
        return entry != null && entry.term() == term;
    }

    private boolean logUpToDate(long index, long term) throws Exception {
        Entry last = ctx.replicatedLog().last();
        return last == null || term > last.term() ||
                (term == last.term() && index > last.index());
    }

}
