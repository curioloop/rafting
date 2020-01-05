package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import io.lubricant.consensus.raft.support.RaftConfig;

import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class FileMachineProvider implements MachineProvider {

    private RaftConfig config;
    private Map<String, FileMachine> machineMap;

    public FileMachineProvider(RaftConfig config) throws Exception {
        if (! Files.exists(config.statePath())) {
            Files.createDirectories(config.statePath());
        }
        this.config = config;
        machineMap = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized RaftMachine bootstrap(String contextId, RaftLog raftLog) throws Exception {
        FileMachine machine = machineMap.get(contextId);
        if (machine == null) {
            machine = new FileMachine(config.statePath().resolve(contextId).toString(), new RocksSerializer());
            machineMap.put(contextId, machine);
        }
        return machine;
    }

    @Override
    public synchronized void close() throws Exception {
        machineMap.clear();
    }

}
