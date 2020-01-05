package io.lubricant.consensus.raft.command.storage;

import io.lubricant.consensus.raft.command.RaftClient.Command;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.support.serial.Serialization;
import io.lubricant.consensus.raft.support.serial.SerializeException;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RocksLog implements RaftLog, Closeable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RocksLog.class);

    static class Logger extends org.rocksdb.Logger {
        public Logger(Options options) {
            super(options);
        }
        @Override
        protected void log(InfoLogLevel level, String message) {
            switch (level) {
                // case DEBUG_LEVEL: logger.debug(message); break;
                // case INFO_LEVEL: logger.info(message); break;
                case WARN_LEVEL: logger.warn(message); break;
                case ERROR_LEVEL: case FATAL_LEVEL: case HEADER_LEVEL:
                case NUM_INFO_LOG_LEVELS: logger.error(message); break;
            }
        }
    }

    private RocksDB db;
    private RocksSerializer serializer;
    private volatile long commitIndex;
    private volatile Entry lastEntry;
    private volatile boolean closed;

    public RocksLog(String path, RocksSerializer serializer) throws Exception {
        Options options = new Options().setCreateIfMissing(true);
        options.setManualWalFlush(true);
        options.setLogger(new Logger(options));
        this.db = RocksDB.open(options, path);
        this.serializer = serializer;
        this.lastEntry = lastEntry();
    }

    @Override
    public Entry newEntry(long term, Command cmd) throws Exception {
        Entry last = last();
        long index = last == null ? 1: last.index() + 1;
        byte[] value = serializer.serialize(longToBytes(term), cmd);
        db.put(longToBytes(index), value);
        db.flushWal(true);
        return lastEntry = new RocksEntry(term, index, value);
    }

    @Override
    public long lastCommitted() throws Exception {
        if (closed) {
            throw new IllegalStateException("log closed");
        }
        return commitIndex;
    }

    @Override
    public boolean markCommitted(long commitIndex) throws Exception {
        if (commitIndex < this.commitIndex) {
            throw new AssertionError("rollback is not allowed");
        }
        if (commitIndex > this.commitIndex) {
            this.commitIndex = commitIndex;
            return true;
        }
        return false;
    }

    @Override
    public Entry last() throws Exception {
        return lastEntry;
    }

    @Override
    public Entry get(long index) throws Exception {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        byte[] value = db.get(longToBytes(index));
        return (value == null) ? null: new RocksEntry(bytesToLong(value), index, value);
    }

    @Override
    public Entry[] batch(long index, int length) throws Exception {
        List<byte[]> keys = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            long cursor = index + i;
            if (cursor != 0) { // ignore 0 index
                keys.add(longToBytes(cursor));
            } else if (i > 0) {
                throw new IllegalStateException("overflow");
            }
        }

        List<byte[]> values = db.multiGetAsList(keys);
        if (keys.size() != values.size()) {
            throw new IllegalStateException("size");
        }

        List<Entry> entries = new ArrayList<>(length);
        for (int i = 0; i < keys.size(); i++) {
            byte[] value = values.get(i);
            if (value != null) {
                entries.add(new RocksEntry(bytesToLong(value), bytesToLong(keys.get(i)), value));
            } else if (i+1 < values.size() && values.get(i+1) != null) {
                throw new AssertionError("found vacancy in log");
            }
        }
        return entries.toArray(new Entry[0]);
    }

    @Override
    public void append(Entry[] entries) throws Exception {
        Entry last = entries[entries.length - 1];
        RocksIterator iterator = db.newIterator();
        iterator.seekForPrev(longToBytes(last.index()));
        long prevLogIndex = 0;
        if (iterator.isValid()) {
            prevLogIndex = bytesToLong(iterator.key());
        } else if (entries[0].index() != 1) {
            throw new AssertionError("log index should start from 1");
        }
        Entry prev = null;
        for (Entry entry : entries) {
            if (prev != null && prev.index() != entry.index() - 1) {
                throw new AssertionError("log index is not continuous");
            }
            if (entry.index() > prevLogIndex) {
                if (prev == null || prev.index() == prevLogIndex) {
                    if (prevLogIndex != entry.index() - 1) {
                        throw new AssertionError("log index is not continuous");
                    }
                }
                RocksEntry e = (RocksEntry) entry;
                db.put(longToBytes(e.index()), e.data);
            }
            prev = entry;
        }
        lastEntry = lastEntry();
        db.flushWal(true);
    }

    @Override
    public Entry conflict(Entry[] entries) throws Exception {
        byte[] term = new byte[Long.BYTES];
        Entry prev = null;
        for (Entry entry : entries) { // 从前向后遍历
            if (prev != null && prev.index() != entry.index() - 1) {
                throw new AssertionError("log index is not continuous");
            }
            int size = db.get(longToBytes(entry.index()), term);
            if (size == RocksDB.NOT_FOUND) { // 该 entry 之前的 entry 一一对应
                return null;
            }
            if (bytesToLong(term) != entry.term()) {
                return new RocksEntry(bytesToLong(term), entry.index(), null);
            }
            prev = entry;
        }
        return null; // 所有 entry 都一一对应
    }

    @Override
    public void truncate(long index) throws Exception {
        Entry last = last();
        if (last != null && last.index() >= index) {
            db.deleteRange(longToBytes(index), longToBytes(last.index() + 1));
        }
        lastEntry = lastEntry();
    }

    private Entry lastEntry() {
        RocksIterator iterator = db.newIterator();
        iterator.seekToLast();
        if (iterator.isValid()) {
            byte[] key = iterator.key();
            byte[] value = iterator.value();
            return new RocksEntry(bytesToLong(value), bytesToLong(key), value);
        }
        return null;
    }

    static Command entryToCmd(RocksEntry entry) throws SerializeException {
        return Serialization.readObject(Long.BYTES, entry.data);
    }

    static byte[] longToBytes(long index) {
        return new byte[]{
                (byte)(index >>> 56),
                (byte)(index >>> 48),
                (byte)(index >>> 40),
                (byte)(index >>> 32),
                (byte)(index >>> 24),
                (byte)(index >>> 16),
                (byte)(index >>>  8),
                (byte)(index)};
    }

    static long bytesToLong(byte[] key) {
        return (((long)key[0] << 56) +
                ((long)(key[1] & 255) << 48) +
                ((long)(key[2] & 255) << 40) +
                ((long)(key[3] & 255) << 32) +
                ((long)(key[4] & 255) << 24) +
                ((key[5] & 255) << 16) +
                ((key[6] & 255) <<  8) +
                ((key[7] & 255)));
    }

    @Override
    public void close() throws IOException {
        closed = true;
        db.close();
    }
}
