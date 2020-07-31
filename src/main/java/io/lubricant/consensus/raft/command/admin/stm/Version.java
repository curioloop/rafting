package io.lubricant.consensus.raft.command.admin.stm;

public class Version {

    private final long txId;
    private final String value;

    public Version(long txId, String value) {
        this.txId = txId;
        this.value = value;
    }

    public long txId() {
        return txId;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value + "(" + txId + ")";
    }
}
