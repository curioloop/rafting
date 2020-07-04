package io.lubricant.consensus.raft;

import io.lubricant.consensus.raft.command.RaftClient;
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

    private Map<String, RaftClient> clientMap;

    public RaftContainer(String configPath) throws Exception {
        config = new RaftConfig(configPath, true);
        clientMap = new ConcurrentHashMap<>();
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

    public RaftClient getClient(String contextId) throws Exception {
        RaftClient client = clientMap.get(contextId);
        if (client != null && client.refer()) {
            return client;
        }
        if (active) synchronized (this) {
            if (active) {
                RaftContext context = manager.getContext(contextId, true);
                client = new RaftClient(context, clientMap);
                clientMap.put(contextId, client);
            }
        }
        return client;
    }

    public synchronized void destroy() {

        if (!active) {
            return;
        }

        logger.info("Start destroying RaftContainer");

        clientMap.clear();

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
