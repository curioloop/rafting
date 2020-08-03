package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.context.RaftContext;

import java.nio.file.Path;
import java.util.concurrent.Future;

/**
 * 状态机
 */
public interface RaftMachine extends AutoCloseable {

    /**
     * 检查点
     * 记录状态机在执行某条日志后对应的状态
     */
    class Checkpoint {
        private final Path path;
        private final long index;
        public Checkpoint(Path path, long index) {
            this.path = path;
            this.index = index;
        }
        public Path path() { return path; }
        public long lastIncludeIndex() { return index; }
        public void release() { }
    }

    /***
     * 可选初始化
     */
    default void initialize(RaftContext context) throws Exception {}

    /**
     * 最近一条已执行日志记录的索引
     */
    long lastApplied();

    /**
     * 执行日志命令（同时更新 lastApplied）
     * 原子性操作，可以看作是一个事务
     */
    Object apply(Entry entry) throws Exception;

    /**
     * 生成检查点（必须包含 mustIncludeIndex 给定的日志）
     * 异步、同步生成均支持（返回 null 则不触发日志压缩）
     * */
    Future<Checkpoint> checkpoint(long mustIncludeIndex) throws Exception;

    /**
     * 从检查点恢复
     * 原子性操作
     */
    void recover(Checkpoint checkpoint) throws Exception;

    /**
     * 清空数据
     */
    default void destroy() throws Exception {}

}
