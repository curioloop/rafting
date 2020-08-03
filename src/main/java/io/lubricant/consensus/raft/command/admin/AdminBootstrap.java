package io.lubricant.consensus.raft.command.admin;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.command.storage.RocksStateLoader;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.support.RaftConfig;

public class AdminBootstrap implements StateLoader, MachineProvider {

    private RaftConfig config;
    private ContextManager manager;

    private RocksStateLoader stateLoader;
    private Administrator administrator;

    public AdminBootstrap(RaftConfig config, ContextManager manager) throws Exception {
        this.config = config;
        this.manager = manager;
        stateLoader = new RocksStateLoader(config);
    }

    @Override
    public synchronized Administrator bootstrap(String contextId, RaftLog raftLog) throws Exception {
        if (administrator == null)
            administrator = new Administrator(config.statePath().resolve(contextId), manager);
        return administrator;
    }

    @Override
    public RaftLog restore(String contextId, boolean create) throws Exception {
        return stateLoader.restore(contextId, create);
    }

    @Override
    public synchronized void close() throws Exception {
        if (administrator != null) {
            administrator.close();
        }
        stateLoader.close();
    }
}
