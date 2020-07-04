package io.lubricant.consensus.raft.cluster;

import io.lubricant.consensus.raft.RaftContainer;
import io.lubricant.consensus.raft.cluster.cmd.AppendCommand;
import io.lubricant.consensus.raft.cluster.cmd.FileBasedTestFactory;
import io.lubricant.consensus.raft.command.RaftClient;
import io.lubricant.consensus.raft.context.member.Leadership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class TestNode3 {

    private static final Logger logger = LoggerFactory.getLogger(TestNode3.class);

    public static void main(String[] args) throws Exception {
        RaftContainer container = new RaftContainer("raft3.xml");
        container.create(new FileBasedTestFactory());
        RaftClient root = container.getClient("root");
        while (true) {
            try {
                int rand = ThreadLocalRandom.current().nextInt(100);
                Boolean result = root.execute(new AppendCommand("node3-" + rand), 1000);
                logger.info("execute result: {}", result);
            } catch (Throwable ex) {
                if (ex instanceof ExecutionException) {
                    ex = ex.getCause();
                }
                if (! (ex instanceof Leadership.NotLeaderException)) {
                    logger.info("execute failed: {}", ex.getClass().getSimpleName());
                }
            }
            Thread.sleep(5000);
        }
    }

}
