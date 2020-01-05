package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.support.serial.Serialization;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;

import java.io.*;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;

/**
 * 文件锁（持久化 currentTerm 与 candidateID / 保证单机唯一实例）
 */
public class StableLock implements Closeable {

    private final RandomAccessFile file;
    private final FileLock lock;

    public StableLock(Path path) throws Exception {
        file = new RandomAccessFile(path.toFile(), "rw");
        lock = file.getChannel().tryLock();
        if (lock == null) {
            throw new IllegalStateException("fail to acquire lock");
        }
    }

    public Map.Entry<Long, ID> restore() {
        try {
            if (file.length() > 0) {
                file.seek(0);
                long term = file.readLong();
                int len = file.readInt();
                byte[] bytes = new byte[len];
                file.readFully(bytes);
                ID candidate = Serialization.readObject(bytes);
                return new AbstractMap.SimpleImmutableEntry<>(term, candidate);
            }
            return null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void persist(long term, ID candidate) {
        try {
            byte[] id = Serialization.writeObject(candidate);
            file.seek(0);
            file.writeLong(term);
            file.writeInt(id.length);
            file.write(id);
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
