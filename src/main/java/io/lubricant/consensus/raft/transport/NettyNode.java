package io.lubricant.consensus.raft.transport;


import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.PendingTask;
import io.lubricant.consensus.raft.support.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;
import io.lubricant.consensus.raft.transport.RaftCluster.SnapService;
import io.lubricant.consensus.raft.transport.event.Event;
import io.lubricant.consensus.raft.transport.event.WaitSnapEvent;
import io.lubricant.consensus.raft.transport.rpc.Async;
import io.lubricant.consensus.raft.transport.rpc.AsyncService;
import io.netty.channel.Channel;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * 服务节点
 */
public class NettyNode extends AsyncService {

    private static final Method APPEND_ENTRY;
    private static final Method REQUEST_VOTE;
    private static final Method INSTALL_SNAP;
    static {
        try {
            APPEND_ENTRY = RaftService.class.getDeclaredMethod("appendEntries",
                    long.class, ID.class, long.class, long.class, Entry[].class, long.class);
            REQUEST_VOTE = RaftService.class.getDeclaredMethod("requestVote",
                    long.class, ID.class, long.class, long.class);
            INSTALL_SNAP = RaftService.class.getDeclaredMethod("installSnapshot",
                    long.class, ID.class, long.class, long.class);
        } catch (NoSuchMethodException e) {
            throw new Error(e);
        }
    }

    class ServiceStub implements RaftService, SnapService {

        private final String contextId;

        private ServiceStub(String contextId) {
            this.contextId = contextId;
        }

        @Override
        public Async<RaftResponse> appendEntries(long term, ID leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit) throws Exception {
            String scope = APPEND_ENTRY.getName() + ':' + contextId;
            return invoke(scope, new Object[]{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit});
        }
        @Override
        public Async<RaftResponse> requestVote(long term, ID candidateId, long lastLogIndex, long lastLogTerm) throws Exception {
            String scope = REQUEST_VOTE.getName() + ':' + contextId;
            return invoke(scope, new Object[]{term, candidateId, lastLogIndex, lastLogIndex});
        }
        @Override
        public Async<RaftResponse> installSnapshot(long term, ID leaderId, long lastIncludedIndex, long lastIncludedTerm) throws Exception {
            String scope = INSTALL_SNAP.getName() + ':' + contextId;
            return invoke(scope, new Object[]{term, leaderId, lastIncludedIndex, lastIncludedTerm});
        }
        @Override
        public PendingTask<Snapshot> obtainSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
            return node.wait(new WaitSnapEvent(contextId, lastIncludedIndex, lastIncludedTerm));
        }
    }

    public NettyNode(EventNode node) {
        super(node);
    }

    public ServiceStub getService(String contextId) {
        return new ServiceStub(contextId);
    }

    public Invocation<RaftResponse> getInvocationIfPresent(String scope, int sequence) {
        return remove(scope, sequence);
    }

    public String parseContextId(String scope) {
        if (scope.startsWith(APPEND_ENTRY.getName())) {
            return scope.substring(APPEND_ENTRY.getName().length() + 1);
        }
        if (scope.startsWith(REQUEST_VOTE.getName())) {
            return scope.substring(REQUEST_VOTE.getName().length() + 1);
        }
        if (scope.startsWith(INSTALL_SNAP.getName())) {
            return scope.substring(INSTALL_SNAP.getName().length() + 1);
        }
        throw new IllegalArgumentException("unknown context " + scope);
    }

    public Callable prepareLocalInvocation(String scope, Object data, RaftContext context) {

        if (scope.startsWith(APPEND_ENTRY.getName())) {
            Object[] params = (Object[]) data;
            if (params.length != 6) {
                throw new IllegalArgumentException("illegal argument size " + params.length);
            }
            long term = ((Number) params[0]).longValue();
            ID leaderId = (ID) params[1];
            long prevLogIndex = ((Number) params[2]).longValue();
            long prevLogTerm = ((Number) params[3]).longValue();
            Entry[] entries = (Entry[]) params[4];
            long leaderCommit = ((Number) params[5]).longValue();
            return () ->  context.participant().appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        }
        if (scope.startsWith(REQUEST_VOTE.getName())) {
            Object[] params = (Object[]) data;
            if (params.length != 4) {
                throw new IllegalArgumentException("illegal argument size " + params.length);
            }
            long term = ((Number) params[0]).longValue();
            ID candidateId = (ID) params[1];
            long lastLogIndex = ((Number) params[2]).longValue();
            long lastLogTerm = ((Number) params[3]).longValue();
            return () ->  context.participant().requestVote(term, candidateId, lastLogIndex, lastLogTerm);
        }
        if (scope.startsWith(INSTALL_SNAP.getName())) {
            Object[] params = (Object[]) data;
            if (params.length != 4) {
                throw new IllegalArgumentException("illegal argument size " + params.length);
            }
            long term = ((Number) params[0]).longValue();
            ID candidateId = (ID) params[1];
            long lastIncludedIndex = ((Number) params[2]).longValue();
            long lastIncludedTerm = ((Number) params[3]).longValue();
            return () ->  context.participant().installSnapshot(term, candidateId, lastIncludedIndex, lastIncludedTerm);
        }
        throw new IllegalArgumentException("unknown context " + scope);
    }

    public boolean replyRequest(Event event) {
        Channel channel = node.channel();
        if (channel != null && channel.isWritable()) {
            channel.writeAndFlush(event);
            return true;
        }
        return false;
    }

    public void connect() {
        node.connect();
    }

    public void close() {
        node.close();
    }

}
