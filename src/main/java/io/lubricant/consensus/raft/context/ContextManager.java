package io.lubricant.consensus.raft.context;

import io.lubricant.consensus.raft.command.*;
import io.lubricant.consensus.raft.support.EventLoopGroup;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.command.SnapshotArchive;
import io.lubricant.consensus.raft.support.StableLock;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.transport.RaftCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public synchronized RaftContext createContext(String contextId) throws Exception {
        RaftContext context = contextMap.get(contextId);
        if (context != null) {
            return context;
        }

        logger.info("Start creating RaftContext({})", contextId);
        RaftLog raftLog = null;
        RaftMachine raftMachine = null;
        StableLock lock = null;
        SnapshotArchive snap;
        try {
            if (!Files.exists(config.lockerPath())) {
                Files.createDirectories(config.lockerPath());
            }
            if (!Files.exists(config.snapshotPath())) {
                Files.createDirectories(config.snapshotPath());
            }
            lock = new StableLock(config.lockerPath().resolve(contextId));
            snap = new SnapshotArchive(config.snapshotPath().resolve(contextId), 5);
            raftLog = stateLoader.restore(contextId, true);
            raftMachine = machineProvider.bootstrap(contextId, raftLog);
            RaftContext raftContext = new RaftContext(contextId, lock, snap, config, raftLog, raftMachine);
            raftContext.initialize(cluster, routine, eventLoops.next(raftContext)).get(); // block till initialization finished
            context = raftContext;
        } catch (Exception ex) {
            if (raftMachine != null) {
                try { raftMachine.close(); } catch (Exception e) {
                    logger.error("Close RaftContext({}) raft machine failed", contextId, e);
                }
            }
            if (raftLog != null) {
                try { raftLog.close(); } catch (Exception e) {
                    logger.error("Close RaftContext({}) raft log failed", contextId, e);
                }
            }
            if (lock != null) {
                try { lock.close(); } catch (IOException e) {
                    logger.error("Close RaftContext({}) lock file failed", contextId, e);
                }
            }

            logger.info("Create RaftContext({}) failed", contextId);
            throw ex;
        }

        logger.info("Create RaftContext({}) successfully", contextId);
        contextMap.put(contextId, context);
        return context;
    }

    /**
     * 销毁上下文
     * @param contextId 上下文 ID
     */
    public synchronized boolean destroyContext(String contextId) throws Exception {
        RaftContext context = contextMap.remove(contextId);
        if (context == null) return false;

        logger.info("Start destroying RaftContext({})", contextId);
        RaftLog raftLog = context.replicatedLog();
        RaftMachine raftMachine = context.stateMachine();
        context.close(false);
        try {
            Files.delete(config.lockerPath().resolve(contextId));
        } catch (Exception e) {
            logger.error("Destroy RaftContext({}) lock file failed", contextId, e);
        }
        try {
            Iterator<Path> it = Files.list(config.snapshotPath().resolve(contextId)).iterator();
            while (it.hasNext()) Files.deleteIfExists(it.next());
            Files.delete(config.snapshotPath().resolve(contextId));
        } catch (Exception e) {
            logger.error("Destroy RaftContext({}) snap arch failed", contextId, e);
        }
        try { raftLog.destroy(); } catch (Exception e) {
            logger.error("Destroy RaftContext({}) raft log failed", contextId, e);
        }
        try { raftMachine.destroy(); } catch (Exception e) {
            logger.error("Destroy RaftContext({}) raft machine failed", contextId, e);
        }
        logger.info("Destroy RaftContext({}) finished", contextId);
        return true;
    }

    /**
     * 获取上下文
     * @param contextId 上下文 ID
     */
    public RaftContext getContext(String contextId) throws Exception {
        return contextMap.get(contextId);
    }

    @Override
    public synchronized void close() throws Exception {
        eventLoops.shutdown(true);
        Iterator<Map.Entry<String, RaftContext>> contextIt = contextMap.entrySet().iterator();
        while (contextIt.hasNext()) {
            RaftContext context = contextIt.next().getValue();
            context.close(true);
            contextIt.remove();
        }
        eventLoops.shutdown(false);
        routine.close();
    }

}
