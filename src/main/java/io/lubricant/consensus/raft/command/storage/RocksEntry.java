package io.lubricant.consensus.raft.command.storage;

import io.lubricant.consensus.raft.command.RaftLog;

public class RocksEntry implements RaftLog.Entry {

    private final long term;
    private final long index;
    final byte[] data;

    public RocksEntry(long term, long index, byte[] data) {
        this.term = term;
        this.index = index;
        this.data = data;
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
    public String toString() {
        return "RocksEntry(" + index + ":" + term + ")";
    }

}
