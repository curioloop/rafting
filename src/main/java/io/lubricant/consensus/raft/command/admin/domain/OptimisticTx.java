package io.lubricant.consensus.raft.command.admin.domain;

import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.admin.stm.Revision;
import io.lubricant.consensus.raft.command.admin.stm.STM;

import java.util.Map;

public class OptimisticTx implements RaftStub.Command<Boolean> {

    private final long txId;
    private final Map<Revision, String> modSet;

    public OptimisticTx(STM stm) {
        this.txId = stm.txId();
        this.modSet = stm.mod();
    }

    public long txId() {
        return txId;
    }

    public Map<Revision, String> modSet() {
        return modSet;
    }
}