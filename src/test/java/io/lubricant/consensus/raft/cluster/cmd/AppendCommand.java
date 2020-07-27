package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.RaftStub.Command;

public class AppendCommand implements Command<Boolean> {

    private final String line;

    public AppendCommand(String line) {
        this.line = line;
    }

    public String line() {
        return line;
    }

}
