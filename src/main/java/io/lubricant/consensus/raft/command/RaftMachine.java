package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.command.RaftLog.Entry;

/**
 * 状态机
 */
public interface RaftMachine extends AutoCloseable {

    /**
     * 最近一条已执行日志记录的索引
     */
    long lastApplied();

    /**
     * 执行日志命令（同时更新 lastApplied）
     * 操作是原子性的，可以看作是一个事务
     */
    Object apply(Entry entry) throws Exception;

}
