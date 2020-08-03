package io.lubricant.consensus.raft.support;


import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import io.lubricant.consensus.raft.command.storage.RocksStateLoader;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;
import io.lubricant.consensus.raft.transport.NettyCluster;
import io.lubricant.consensus.raft.transport.RaftCluster;

/**
 * 全局工厂（产生容器所依赖的所有组件）
 */
public abstract class RaftFactory {

    public StateLoader loadState(RaftConfig config) throws Exception {
        return new RocksStateLoader(config);
    }

    public ContextManager resumeContext(RaftConfig config) throws Exception {
        return new ContextManager(config);
    }

    public RaftCluster joinCluster(RaftConfig config) throws Exception {
        return new NettyCluster(config);
    }

    public void bootstrap(RaftCluster cluster, ContextManager manager,
                          StateLoader loader, MachineProvider provider) throws Exception {
        manager.start(cluster, loader, provider);
        ((NettyCluster) cluster).start(manager);
    }

    public abstract MachineProvider restartMachine(RaftConfig config) throws Exception;

}
