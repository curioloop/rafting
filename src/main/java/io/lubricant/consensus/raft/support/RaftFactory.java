package io.lubricant.consensus.raft.support;


import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.transport.RaftCluster;

/**
 * 全局工厂（产生容器所依赖的所有组件）
 */
public interface RaftFactory {

    StateLoader loadState(RaftConfig config) throws Exception;

    MachineProvider restartMachine(RaftConfig config) throws Exception;

    ContextManager resumeContext(RaftConfig config) throws Exception;

    RaftCluster joinCluster(RaftConfig config) throws Exception;

    default void bootstrap(RaftCluster cluster, ContextManager manager,
                           StateLoader loader, MachineProvider provider) {
        manager.start(cluster, loader, provider);
    }

}
