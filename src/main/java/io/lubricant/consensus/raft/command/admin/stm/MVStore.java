package io.lubricant.consensus.raft.command.admin.stm;

public interface MVStore {

    Version get(String key);

}
