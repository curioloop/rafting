package io.lubricant.consensus.raft.command.admin.stm;

import java.util.HashMap;
import java.util.Map;

public class STM {

    private final long txId;
    private final MVStore store;

    private Map<String, Version> readSet = new HashMap<>();
    private Map<String, String> writeSet = new HashMap<>();

    public STM(long txId, MVStore store) {
        this.txId = txId;
        this.store = store;
    }

    public long txId() {
        return txId;
    }

    public String get(String key) {
        if (writeSet.containsKey(key)) {
            return writeSet.get(key); // allow null
        }
        Version version = readSet.get(key);
        if (version == null) {
            version = store.get(key);
            readSet.put(key, version);
        }
        return version.value();
    }

    public void put(String key, String value) {
        writeSet.put(key, value);
    }

    public Map<Revision, String> mod() {
        Map<Revision, String> modSet = new HashMap<>();
        for (Map.Entry<String, String> e : writeSet.entrySet()) {
            Version version = readSet.get(e.getKey());
            modSet.put(new Revision(version == null ? -1: version.txId(), e.getKey()), e.getValue());
        }
        for (Map.Entry<String, Version> e : readSet.entrySet()) {
            if (! writeSet.containsKey(e.getKey())) {
                modSet.put(new Revision(e.getValue().txId(), e.getKey()), null);
            }
        }
        return modSet;
    }

}
