package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftFactory;

public class FileBasedTestFactory extends RaftFactory {

    @Override
    public MachineProvider restartMachine(RaftConfig config) throws Exception {
        return new FileMachineProvider(config);
    }

}
