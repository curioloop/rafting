package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftLog.EntryKey;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.support.serial.Serialization;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;

import java.io.*;
import java.nio.channels.FileLock;
import java.nio.file.Path;

/**
 * 文件锁:
 * 1. 保证单机唯一实例
 * 2. 持久化以下属性：
 *    - currentTerm: 已知最新的任期
 *    - candidateID: 最近一次投票的候选者
 *    - stateMilestone: 状态机快照里程碑
 */
public class StableLock implements Closeable {

    public static class Persistence {

        public final long term;
        public final ID ballot;
        public final Entry milestone;

        private Persistence(long term, ID ballot, EntryKey milestone) {
            this.term = term;
            this.ballot = ballot;
            this.milestone = milestone;
        }

    }

    private final RandomAccessFile file;
    private final FileLock lock;

    public StableLock(Path path) throws Exception {
        file = new RandomAccessFile(path.toFile(), "rw");
        lock = file.getChannel().tryLock();
        if (lock == null) {
            throw new IllegalStateException("fail to acquire lock");
        }
        if (file.length() == 0) {
            file.write(new byte[28]);
        }
    }

    public synchronized Persistence restore() {
        try {
            file.seek(0);
            EntryKey epoch = new EntryKey(file.readLong(), file.readLong());
            long term = file.readLong();
            int length = file.readInt();
            ID candidate = null;
            if (length > 0) {
                byte[] bytes = new byte[length];
                file.readFully(bytes);
                candidate = Serialization.readObject(bytes);
            }
            return new Persistence(term, candidate, epoch);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized void persist(long term, ID candidate) {
        try {
            byte[] id = Serialization.writeObject(candidate);
            file.seek(16);
            file.writeLong(term);
            file.writeInt(id.length);
            file.write(id);
            file.getFD().sync();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized void persist(Snapshot milestone) {
        try {
            file.seek(0);
            file.writeLong(milestone.lastIncludeIndex());
            file.writeLong(milestone.lastIncludeTerm());
            file.getFD().sync();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        lock.release();
        file.close();
    }

}
