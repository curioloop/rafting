package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.command.storage.RocksStateLoader;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftFactory;
import io.lubricant.consensus.raft.transport.RaftCluster;
import io.lubricant.consensus.raft.transport.NettyCluster;

public class FileBasedTestFactory implements RaftFactory {

    @Override
    public StateLoader loadState(RaftConfig config) throws Exception {
        return new RocksStateLoader(config);
    }

    @Override
    public MachineProvider restartMachine(RaftConfig config) throws Exception {
        return new FileMachineProvider(config);
    }

    @Override
    public ContextManager resumeContext(RaftConfig config) throws Exception {
        return new ContextManager(config);
    }

    @Override
    public RaftCluster joinCluster(RaftConfig config) throws Exception {
        return new NettyCluster(config);
    }

    @Override
    public void bootstrap(RaftCluster cluster, ContextManager manager, StateLoader loader, MachineProvider provider) {
        manager.start(cluster, loader, provider);
        ((NettyCluster) cluster).start(manager);
    }
}
