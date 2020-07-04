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


    /*public static void main(String[] args) throws Exception {

        System.out.println(SnapshotArchive.supportAtomicMove);

        Path dir = Paths.get("/data0/file-machine-test");
        if (Files.exists(dir)) {
            for (Path path : Files.list(dir).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        } else {
            Files.createDirectory(dir);
        }

        SnapshotArchive archive = new SnapshotArchive(Paths.get("/data0"), "file-machine-test", 2);

        FileMachine machine = new FileMachine(dir.resolve("appender").toString(), new RocksSerializer());
        machine.apply(1, new AppendCommand("1"));
        machine.apply(2, new AppendCommand("2"));
        machine.apply(3, new AppendCommand("3"));

        RaftMachine.Checkpoint checkpoint = machine.checkpoint(1).get();
        System.out.printf("index: %d\n", checkpoint.lastIncludeIndex());
        archive.takeSnapshot(checkpoint, 1);
        System.out.println(archive.lastSnapshot().path());

        SnapshotArchive.Snapshot snapshot = archive.lastSnapshot();
        FileMachine acceptor = new FileMachine(dir.resolve("acceptor").toString(), new RocksSerializer());
        acceptor.recover(snapshot);
        acceptor.apply(4, new AppendCommand("4"));
        acceptor.apply(5, new AppendCommand("5"));
        checkpoint = acceptor.checkpoint(4).get();
        System.out.printf("index: %d\n", checkpoint.lastIncludeIndex());
        archive.takeSnapshot(checkpoint, 2);
        System.out.println(archive.lastSnapshot().path());

        snapshot = archive.lastSnapshot();
        machine.recover(snapshot);
        machine.apply(6, new AppendCommand("6"));
        machine.apply(7, new AppendCommand("7"));
        checkpoint = machine.checkpoint(7).get();
        System.out.printf("index: %d\n", checkpoint.lastIncludeIndex());
        archive.takeSnapshot(checkpoint, 3);
        System.out.println(archive.lastSnapshot().path());
    }*/

}
