package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.RaftService;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
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

public class NettyCluster implements RaftCluster, EventBus.EventDispatcher {

    private final static Logger logger = LoggerFactory.getLogger(NettyCluster.class);

    private NodeID localID;
    private EventBus eventBus;
    private Map<NodeID, NettyNode> remoteNodes;
    private NioEventLoopGroup eventLoops;
    private ContextManager contextManager;

    public NettyCluster(RaftConfig config) {

        URI localURI = config.localURI();
        NodeID local = new NodeID(localURI.getHost(), localURI.getPort());

        List<URI> remoteURIs = config.remoteURIs();
        List<NodeID> remote = remoteURIs.stream().map(uri -> new NodeID(uri.getHost(), uri.getPort())).collect(Collectors.toList());

        localID = local;
        eventBus = new EventBus(local.port(), this);
        eventLoops = new NioEventLoopGroup(remote.size(),
                RaftThreadGroup.instance().newFactory("EventNodeGroup-%d"));
        remoteNodes = new HashMap<>();
        for (NodeID nodeID : remote) {
            remoteNodes.put(nodeID,
                    new NettyNode(new EventNode(local, nodeID, eventLoops)));
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
            logger.error("source node is not found: " + source.nodeID());
            return;
        }

        try {
            String contextId = node.parseContextId(source.scope());
            RaftContext context = contextManager.getContext(contextId);
            Callable invocation = node.prepareLocalInvocation(source.scope(), event.message(), context);
            context.eventLoop().execute(() -> {
                try {
                    node.replyRequest(new PongEvent(source, invocation.call(), event.sequence()));
                } catch (Exception e) {
                    logger.error("fail to process request from: " + source, e);
                }
            });
        } catch (Exception e) {
            logger.error("fail to dispatch request from: " + source, e);
        }
    }

    @Override
    public void on(PongEvent event) {
        EventID source = event.source();
        NettyNode node = remoteNodes.get(source.nodeID());
        if (node == null) {
            logger.error("source node is not found: " + source.nodeID());
            return;
        }
        AsyncService.Invocation<RaftResponse> invocation =
                node.getInvocationIfPresent(source.scope(), event.sequence());
        if (invocation != null) {
            invocation.accept(event, RaftResponse.class);
        }
    }

    @Override
    public void on(OneWayEvent event) {
        throw new UnsupportedOperationException();
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
    public RaftService remoteService(ID nodeID, String ctxID) {
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
