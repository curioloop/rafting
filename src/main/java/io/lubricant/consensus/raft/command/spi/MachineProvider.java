package io.lubricant.consensus.raft.command.spi;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftMachine;

/**
 * 状态机工厂（用户可以指定自己的 RaftMachine 实现）
 */
public interface MachineProvider extends AutoCloseable {

    RaftMachine bootstrap(String contextId, RaftLog raftLog) throws Exception;

}
