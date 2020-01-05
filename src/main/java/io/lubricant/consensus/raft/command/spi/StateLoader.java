package io.lubricant.consensus.raft.command.spi;

import io.lubricant.consensus.raft.command.RaftLog;

/**
 * 日志工厂（用户可以指定自己的 RaftLog 实现）
 */
public interface StateLoader extends AutoCloseable {

    RaftLog restore(String contextId, boolean create) throws Exception;

}
