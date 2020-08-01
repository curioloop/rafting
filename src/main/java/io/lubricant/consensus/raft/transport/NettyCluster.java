package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftThreadGroup;
import io.lubricant.consensus.raft.transport.event.*;
import io.lubricant.consensus.raft.transport.rpc.AsyncService;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class NettyCluster implements RaftCluster, EventBus.EventDispatcher {

    private final static Logger logger = LoggerFactory.getLogger(NettyCluster.class);

    private NodeID localID;
    private EventBus eventBus;
    private Map<NodeID, NettyNode> remoteNodes;
    private NioEventLoopGroup eventLoops;
    private NioEventLoopGroup snapEventLoop;
    private ContextManager contextManager;

    public NettyCluster(RaftConfig config) {

        URI localURI = config.localURI();
        NodeID local = new NodeID(localURI.getHost(), localURI.getPort());

        List<URI> remoteURIs = config.remoteURIs();
        List<NodeID> remote = remoteURIs.stream().map(uri -> new NodeID(uri.getHost(), uri.getPort())).collect(Collectors.toList());

        localID = local;
        eventBus = new EventBus(local, this);
        eventLoops = new NioEventLoopGroup(remote.size(), RaftThreadGroup.instance().newFactory("EventNodeGrp-%d"));
        snapEventLoop = new NioEventLoopGroup(1, RaftThreadGroup.instance().newFactory("SnapNodeGrp-%d"));
        remoteNodes = new HashMap<>();
        for (NodeID nodeID : remote) {
            remoteNodes.put(nodeID,
                    new NettyNode(new EventNode(local, nodeID, eventLoops, snapEventLoop)));
        }
        remoteNodes = Collections.unmodifiableMap(remoteNodes);
    }

    public void start(ContextManager contextManager) {
        this.contextManager = contextManager;
        remoteNodes.forEach((k,v) -> v.connect());
        eventBus.start();
    }

    @Override
    public void on(PingEvent event) {
        EventID source = event.source();
        NettyNode node = remoteNodes.get(source.nodeID());
        if (node == null) {
            logger.error("Source({}) node not found", source);
            return;
        }

        try {
            String contextId = node.parseContextId(source.scope());
            RaftContext context = contextManager.getContext(contextId);
            if (context == null) {
                logger.error("RaftContext({}) of Source({}) context not found", contextId, source);
                return;
            }
            if (! context.eventLoop().isAvailable()) {
                logger.error("Source({}) context is unavailable", source);
                return;
            }

            Callable invocation = node.prepareLocalInvocation(source.scope(), event.message(), context);
            context.eventLoop().execute(() -> {
                try {
                    node.replyRequest(new PongEvent(source, invocation.call(), event.sequence()));
                } catch (Exception e) {
                    logger.error("Source({}) process request failed", source, e);
                }
            });
        } catch (Exception e) {
            logger.error("Source({}) dispatch request failed", source, e);
        }
    }

    @Override
    public void on(PongEvent event) {
        EventID source = event.source();
        NettyNode node = remoteNodes.get(source.nodeID());
        if (node == null) {
            logger.error("Source({}) node not found", source);
            return;
        }
        AsyncService.Invocation<RaftResponse> invocation =
                node.getInvocationIfPresent(source.scope(), event.sequence());
        if (invocation != null) {
            invocation.accept(event, RaftResponse.class);
        }
    }

    @Override
    public TransSnapEvent on(WaitSnapEvent event) {
        String contextName = event.context();
        try {
            RaftContext context = contextManager.getContext(contextName);
            if (context == null) {
                throw new Exception("context not found");
            }
            Snapshot snapshot = context.snapArchive().lastSnapshot();
            if (snapshot == null) {
                throw new Exception("no available snapshot");
            }
            if (snapshot.lastIncludeIndex() < event.index() ||
                snapshot.lastIncludeTerm() < event.term()) {
                throw new Exception("no eligible snapshot");
            }
            return new TransSnapEvent(snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm(), snapshot.path());
        } catch (Exception e) {
            return new TransSnapEvent(event.index(), event.term(), e.getMessage());
        }
    }

    @Override
    public int size() {
        return remoteNodes.size() + 1;
    }

    @Override
    public NodeID localID() {
        return localID;
    }

    @Override
    public Set<ID> remoteIDs() {
        return (Set) remoteNodes.keySet();
    }

    @Override
    public NettyNode.ServiceStub remoteService(ID nodeID, String ctxID) {
        NettyNode node = remoteNodes.get(nodeID);
        if (node == null) {
            throw new IllegalArgumentException("node not found " + nodeID);
        }
        return node.getService(ctxID);
    }

    @Override
    public void close() throws Exception {
        eventBus.close();
        remoteNodes.forEach((id, node) -> node.close());
    }
}
