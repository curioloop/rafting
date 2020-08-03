package io.lubricant.consensus.raft.command.admin.domain;

import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.admin.stm.MVStore;

public class Echo implements RaftStub.Command<MVStore> {
}
