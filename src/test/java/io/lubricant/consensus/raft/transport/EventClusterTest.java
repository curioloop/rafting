package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.support.PendingTask;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.transport.event.*;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class EventClusterTest {

    private final static Logger logger = LoggerFactory.getLogger(EventClusterTest.class);

    private EventBus eventBus;
    private Map<NodeID, EventNode> eventNodes;
    private NioEventLoopGroup eventLoops;

    // 主要功能：
    // 实现 RPC 调用
    // 超时机制
    public EventClusterTest(NodeID local, List<NodeID> remote, EventBus.EventDispatcher dispatcher) {
        eventBus = new EventBus(local, dispatcher);
        eventLoops = new NioEventLoopGroup(remote.size());
        eventNodes = new HashMap<>();
        for (NodeID nodeID : remote) {
            eventNodes.put(nodeID, new EventNode(local, nodeID, eventLoops, eventLoops));
        }
        eventNodes = Collections.unmodifiableMap(eventNodes);
        eventNodes.forEach((k, v) -> v.connect());
        eventBus.start();
    }

    public boolean publish(Event event) {
        if (event instanceof Event.BinEvent) {
            NodeID nodeID = ((Event.BinEvent) event).source().nodeID();
            EventNode node = eventNodes.get(nodeID);
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

    public PendingTask<Snapshot> install(NodeID nodeID, WaitSnapEvent event) {
        EventNode node = eventNodes.get(nodeID);
        if (node == null) {
            throw new IllegalArgumentException("No such node " + nodeID);
        }
        return node.wait(event);
    }

    static File createTempFile() throws IOException {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("logback.xml");
        File temp = File.createTempFile("snapshot", null);
        FileOutputStream output = new FileOutputStream(temp);
        byte[] buf = new byte[1024];
        int n = stream.read(buf);
        while (n > 0) {
            output.write(buf, 0, n);
            n = stream.read(buf);
        }
        output.close();
        Runtime.getRuntime().addShutdownHook(new Thread(temp::delete));
        return temp;
    }

    public static void main(String[] args) throws Exception {
        File temp = createTempFile();
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
            public TransSnapEvent on(WaitSnapEvent event) {
                try {
                    long index = ThreadLocalRandom.current().nextLong();
                    long term = ThreadLocalRandom.current().nextLong();
                    return new TransSnapEvent(index, term, Paths.get(temp.getPath()));
                } catch (IOException e) {
                    return new TransSnapEvent(-1L, -1L, e.getMessage());
                }
            }
        });

        int i = 0;
        while (true) {
            cluster.publish(new PingEvent(new EventID("testA", nodeID), "A", i++));
            cluster.publish(new PongEvent(new EventID("testB", nodeID), "B", i++));
            PendingTask<Snapshot> pending = cluster.install(nodeID, new WaitSnapEvent("testA", 0L, 0L));
            pending.perform(()->{
                if (pending.isSuccess()) {
                    logger.info("snap {} ", pending.result().path());
                    Files.delete(pending.result().path());
                }
                return null;
            }).get();

            logger.info("Running");
            Thread.sleep(1000);
        }
    }

}
