package io.lubricant.consensus.raft.command.admin.stm;

import io.lubricant.consensus.raft.support.anomaly.SerializeException;
import io.lubricant.consensus.raft.support.serial.Serialization;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVEngine implements MVStore {

    private static final Version NULL = new Version(0, null);

    private volatile long appliedIdx;
    private long transactionID;
    private Map<String, Version> kvStore = new ConcurrentHashMap<>();

    public long appliedIdx() {
        return appliedIdx;
    }

    public Version get(String key) {
        return kvStore.getOrDefault(key, NULL);
    }

    public KVEngine seenIndex(long index) {
        if (index <= appliedIdx) {
            throw new IllegalArgumentException("revoke");
        }
        appliedIdx = index;
        return this;
    }

    public long nextTx(long index) {
        seenIndex(index);
        return ++transactionID;
    }

    public boolean commitTx(long index, long txId, Map<Revision, String> modSet) {
        seenIndex(index);
        // check conflict
        for (Revision rev : modSet.keySet()) {
            Version version = kvStore.get(rev.key());
            long assumeTx = version == null ? 0: version.txId();
            if (rev.txId() != assumeTx) {
                return false;
            }
        }
        // commit
        for (Map.Entry<Revision, String> e : modSet.entrySet()) {
            Revision rev = e.getKey();
            kvStore.put(rev.key(), new Version(txId, e.getValue()));
        }

        return true;
    }

    public void dumpTo(File file) throws IOException, SerializeException {
        try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
            out.writeLong(appliedIdx);
            out.writeLong(transactionID);
            Serialization.writeObject(kvStore, out);
        }
    }

    public void loadFrom(File file) throws IOException, SerializeException {
        long appliedIdx, transactionID;
        Map<String, Version> kvStore;
        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            appliedIdx = in.readLong();
            transactionID = in.readLong();
            kvStore = Serialization.readObject(in, in.length() - 16);
        }
        if (appliedIdx < this.appliedIdx) {
            throw new IllegalArgumentException("revoke");
        }
        this.appliedIdx = appliedIdx;
        this.transactionID = transactionID;
        this.kvStore = new ConcurrentHashMap<>(kvStore);
    }

    @Override
    public KVEngine clone() {
        KVEngine ng = new KVEngine();
        ng.appliedIdx = appliedIdx;
        ng.transactionID = transactionID;
        ng.kvStore.putAll(kvStore);
        return ng;
    }

}
