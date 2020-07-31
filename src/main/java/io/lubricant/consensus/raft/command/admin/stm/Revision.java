package io.lubricant.consensus.raft.command.admin.stm;

import java.util.Objects;

public class Revision {

    private final long txId;
    private final String key;

    public Revision(long txId, String key) {
        this.txId = txId;
        this.key = key;
    }

    public long txId() {
        return txId;
    }

    public String key() {
        return key;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(txId) ^ (key == null ? 0: key.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Revision) {
            Revision rev = (Revision) obj;
            return rev.txId == txId && Objects.equals(rev.key, key);
        }
        return false;
    }

    @Override
    public String toString() {
        return key + "<" + txId + ">";
    }
}
