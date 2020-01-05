package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.transport.event.*;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EventClusterTest {

    private final static Logger logger = LoggerFactory.getLogger(EventClusterTest.class);

    private EventBus eventBus;
    private Map<NodeID, EventNode> remoteNodes;
    private NioEventLoopGroup eventLoops;

    // 主要功能：
    // 实现 RPC 调用
    // 超时机制
    //

    public EventClusterTest(NodeID local, List<NodeID> remote, EventBus.EventDispatcher dispatcher) {
        eventBus = new EventBus(local.port(), dispatcher);
        eventLoops = new NioEventLoopGroup(remote.size());
        remoteNodes = new HashMap<>();
        for (NodeID nodeID : remote) {
            remoteNodes.put(nodeID, new EventNode(local, nodeID, eventLoops));
        }
        remoteNodes = Collections.unmodifiableMap(remoteNodes);
        remoteNodes.forEach((k,v) -> v.connect());
        eventBus.start();
    }

    public boolean publish(Event event) {
        if (event instanceof Event.BinEvent) {
            NodeID nodeID = ((Event.BinEvent) event).source().nodeID();
            EventNode node = remoteNodes.get(nodeID);
            if (node == null) {
                throw new IllegalArgumentException("No such node " + nodeID);
            }
            Channel channel = node.channel();
            if (channel == null) {
                return false;
            }
            channel.writeAndFlush(event);
        }
        return false;
    }

    public static void main(String[] args) throws InterruptedException {
        NodeID nodeID = NodeID.fromString("127.0.0.1:12345");
        EventClusterTest cluster = new EventClusterTest(
                nodeID, Arrays.asList(nodeID), new EventBus.EventDispatcher() {
            @Override
            public void on(PingEvent event) {
                logger.info("ping {} {} {}", event.source(), event.sequence(), event.message());
            }

            @Override
            public void on(PongEvent event) {
                logger.info("pong {} {} {}", event.source(), event.sequence(), event.message());
            }

            @Override
            public void on(OneWayEvent event) {
                logger.info("onw-way {} {}", event.source(), event.message());
            }
        });

        int i = 0;
        while (true) {
            cluster.publish(new PingEvent(new EventID("testA", nodeID), "A", i++));
            cluster.publish(new PongEvent(new EventID("testB", nodeID), "B", i++));
            cluster.publish(new OneWayEvent(new EventID("testC", nodeID), "C"));
            logger.info("Running");
            Thread.sleep(1000);
        }
    }

}
