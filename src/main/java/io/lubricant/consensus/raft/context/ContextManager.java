package io.lubricant.consensus.raft.context;

import io.lubricant.consensus.raft.command.*;
import io.lubricant.consensus.raft.support.EventLoopGroup;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.SnapshotArchive;
import io.lubricant.consensus.raft.support.StableLock;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.transport.RaftCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 上下文管理器
 */
public class ContextManager implements AutoCloseable  {

    private final Logger logger = LoggerFactory.getLogger(ContextManager.class);

    private MachineProvider machineProvider;
    private StateLoader stateLoader;
    private EventLoopGroup eventLoops;

    private RaftConfig config;
    private RaftRoutine routine;
    private RaftCluster cluster;

    private Map<String, RaftContext> contextMap = new ConcurrentHashMap<>();

    public ContextManager(RaftConfig raftConfig) {
        config = raftConfig;
        routine = new RaftRoutine();
        eventLoops = new EventLoopGroup(3, "ContextLoop");
    }

    public void start(RaftCluster raftCluster, StateLoader stateLoader, MachineProvider machineProvider) {
        cluster = raftCluster;
        this.stateLoader = stateLoader;
        this.machineProvider = machineProvider;
        eventLoops.start();
    }

    /**
     * 创建上下文
     * @param contextId 上下文 ID
     */
    private synchronized RaftContext createContext(String contextId) throws Exception {
        RaftContext context = contextMap.get(contextId);
        if (context != null) {
            return context;
        }

        logger.info("Start creating RaftContext({})", contextId);
        StableLock lock = null;
        SnapshotArchive snap = null;
        RaftLog raftLog = null;
        RaftMachine raftMachine = null;
        try {
            if (!Files.exists(config.lockerPath())) {
                Files.createDirectories(config.lockerPath());
            }
            if (!Files.exists(config.snapshotPath())) {
                Files.createDirectories(config.snapshotPath());
            }
            lock = new StableLock(config.lockerPath().resolve(contextId));
            snap = new SnapshotArchive(config.snapshotPath(), contextId, 5);
            raftLog = stateLoader.restore(contextId, true);
            raftMachine = machineProvider.bootstrap(contextId, raftLog);
            RaftContext raftContext = new RaftContext(contextId, lock, snap, config, raftLog, raftMachine);
            raftContext.initialize(cluster, routine, eventLoops.next());
            context = raftContext;
        } catch (Exception ex) {
            if (raftMachine != null) {
                try { raftMachine.close(); } catch (Exception e) {
                    logger.error("Close raft machine failed", e);
                }
            }
            if (raftLog != null) {
                try { raftLog.close(); } catch (Exception e) {
                    logger.error("Close raft log failed", e);
                }
            }
            if (lock != null) {
                try { lock.close(); } catch (IOException e) {
                    logger.error("Close lock file failed", e);
                }
            }

            logger.info("Create RaftContext({}) failed", contextId);
            throw ex;
        }

        logger.info("RaftContext({}) created successfully", contextId);
        contextMap.put(contextId, context);
        return context;
    }

    /**
     * 获取上下文
     * @param contextId 上下文 ID
     */
    public RaftContext getContext(String contextId, boolean create) throws Exception {
        RaftContext context = contextMap.get(contextId);
        if (context != null)
            return context;
        return createContext(contextId);
    }

    @Override
    public synchronized void close() throws Exception {
        eventLoops.shutdown();
        routine.close();
        Iterator<Map.Entry<String, RaftContext>> contextIt = contextMap.entrySet().iterator();
        while (contextIt.hasNext()) {
            RaftContext context = contextIt.next().getValue();
            context.destroy();
            contextIt.remove();
        }
    }

}
