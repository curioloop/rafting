package io.lubricant.consensus.raft;

import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftFactory;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.transport.RaftCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 容器（管理组件的生命周期）
 */
public class RaftContainer {

    private final static Logger logger = LoggerFactory.getLogger(RaftContainer.class);

    private boolean active;
    private RaftConfig config;

    private StateLoader loader;
    private MachineProvider provider;
    private ContextManager manager;
    private RaftCluster cluster;

    private Map<String, RaftStub> stubMap;

    public RaftContainer(String configPath) throws Exception {
        config = new RaftConfig(configPath, true);
        stubMap = new ConcurrentHashMap<>();
    }

    public synchronized void create(RaftFactory factory) throws Exception {
        logger.info("Start creating RaftContainer");
        try {
            active = true;
            loader = factory.loadState(config);
            provider = factory.restartMachine(config);
            manager = factory.resumeContext(config);
            cluster = factory.joinCluster(config);
            factory.bootstrap(cluster, manager, loader, provider);
            Runtime.getRuntime().addShutdownHook(new Thread(this::destroy, "RaftContainerHook"));
        } catch (Exception e) {
            logger.info("Create RaftContainer failed");
            destroy();
            throw e;
        }
        logger.info("RaftContainer created successfully");
    }


    public void createContext(String contextId) throws Exception {
        if (!active) {
            throw new IllegalStateException("Container closed");
        }
        manager.createContext(contextId);
    }

    public void destroyContext(String contextId) throws Exception {
        if (!active) {
            throw new IllegalStateException("Container closed");
        }
        manager.destroyContext(contextId);
    }

    public RaftStub getStub(String contextId) throws Exception {
        RaftStub stub = stubMap.get(contextId);
        if (stub != null && stub.refer()) {
            return stub;
        }
        if (active) synchronized (this) {
            if (active) {
                RaftContext context = manager.getContext(contextId);
                if (context == null) {
                    return null;
                }
                stub = new RaftStub(context, stubMap);
                stubMap.put(contextId, stub);
            }
        }
        return stub;
    }

    public synchronized void destroy() {

        if (!active) {
            return;
        }

        logger.info("Start destroying RaftContainer");

        stubMap.clear();

        if (cluster != null) {
            try { cluster.close(); }
            catch (Exception e) {
                logger.error("Close cluster failed", e);
            }
        }
        if (manager != null) {
            try { manager.close(); }
            catch (Exception e) {
                logger.error("Close manager failed", e);
            }
        }
        if (provider != null) {
            try { provider.close(); }
            catch (Exception e) {
                logger.error("Close provider failed", e);
            }
        }
        if (loader != null) {
            try { loader.close(); }
            catch (Exception e) {
                logger.error("Close loader failed", e);
            }
        }
        cluster = null;
        manager = null;
        provider = null;
        loader = null;
        active = false;

        logger.info("RaftContainer is destroyed");
    }
}
