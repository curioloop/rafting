package io.lubricant.consensus.raft.command;

import java.io.Serializable;
import java.util.concurrent.Future;

import io.lubricant.consensus.raft.command.RaftClient.Command;

/**
 * 复制日志
 */
public interface RaftLog extends AutoCloseable {

    /**
     * 日志条目
     * 索引 0 用于表示一个不存在的条目，实际的日志索引从 1 开始
     */
    interface Entry extends Serializable {
        long index();
        long term();
    }

    /**
     * 日志条目 Key
     */
    class EntryKey implements Entry, Comparable<EntryKey> {

        private final long index, term;

        public EntryKey(long index, long term) {
            this.index = index;
            this.term = term;
        }

        public EntryKey(Entry entry) {
            this(entry.index(), entry.term());
        }

        @Override
        public long index() {
            return index;
        }

        @Override
        public long term() {
            return term;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(index ^ term);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof EntryKey) {
                EntryKey key = (EntryKey) obj;
                return index == key.index && term == key.term;
            }
            return false;
        }

        @Override
        public int compareTo(EntryKey o) {
            return index != o.index ? Long.compare(index, o.index) : Long.compare(term, o.term);
        }
    }

    /**
     * 新建条目并追加到日志末尾
     * 单线程访问
     */
    Entry newEntry(long term, Command cmd) throws Exception;

    /**
     * 最近一条已提交日志记录的索引
     */
    long lastCommitted() throws Exception;

    /**
     * 保存一条已提交日志记录的索引
     * 单线程访问
     */
    boolean markCommitted(long commitIndex) throws Exception;

    /**
     * 日志起点（该位置的日志已被归档）
     */
    Entry epoch() throws Exception;

    /**
     * 最后一条日志（没有任何日志时返回 null）
     */
    Entry last() throws Exception;

    /**
     * 根据索引获取日志（索引越界时返回 null）
     */
    Entry get(long index) throws Exception;

    /**
     * 根据索批量引获取日志（索引越界时截断）
     */
    Entry[] batch(long index, int length) throws Exception;

    /**
     * 追加日志
     * 单线程访问
     */
    void append(Entry[] entries) throws Exception;

    /**
     * 查找首个冲突的日志条目
     * 单线程访问
     */
    Entry conflict(Entry[] entries) throws Exception;

    /**
     * 删除索引对应及其之后的条目
     * 单线程访问
     */
    void truncate(long index) throws Exception;

    /**
     * 清空日志数据至指定位置（index 单调递增）
     * 单线程访问
     */
    Future<Boolean> flush(long index) throws Exception;
}
