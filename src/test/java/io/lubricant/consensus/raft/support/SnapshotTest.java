package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.cluster.cmd.AppendCommand;
import io.lubricant.consensus.raft.cluster.cmd.FileMachine;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

public class SnapshotTest {

    private SnapshotArchive archive;
    private FileMachine machine;

    @Before
    public void setUp() throws IOException {
        Path dir = Paths.get("/data0/snapshot-archive-test");
        if (Files.exists(dir)) {
            for (Path path : Files.list(dir).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        } else {
            Files.createDirectory(dir);
        }
        archive = new SnapshotArchive(Paths.get("/data0"), "snapshot-archive-test", 5);
        machine = new FileMachine(dir.resolve("machine").toString(), new RocksSerializer());
        machine.apply(1, new AppendCommand("1"));
        machine.apply(2, new AppendCommand("2"));
        machine.apply(3, new AppendCommand("3"));
    }


    private PendingTask<SnapshotArchive.Snapshot> getTask(boolean success) throws Exception {
        Future<RaftMachine.Checkpoint> future = machine.checkpoint(machine.lastApplied());
        return new PendingTask<SnapshotArchive.Snapshot>() {
            @Override
            protected void perform() {
                TimeLimited.newTimer(5000).schedule(()->{
                    RaftMachine.Checkpoint checkpoint = null;
                    if (! success) {
                        setException(new RejectedExecutionException());
                        return;
                    }
                    try {
                        checkpoint = future.get();
                        set(new SnapshotArchive.Snapshot(checkpoint.path(), checkpoint.lastIncludeIndex(), 1));
                    } catch (Exception e) {
                        setException(e);
                        checkpoint.release();
                    }
                });
            }
        };
    }

    @Test
    public void test() throws Exception {
        int index = 3;
        SnapshotArchive.PendingSnapshot ps = archive.pendSnapshot(machine.lastApplied(), 1, getTask(false));
        Assert.assertEquals(ps, archive.pendSnapshot(machine.lastApplied(), 1, null));
        while (true) {
            if (ps.isPending()) {
                System.out.println("task pending...");
                Thread.sleep(1000);
                index++;
                machine.apply(index, new AppendCommand(""+index));
            } else{
                System.out.println("task finish : " + ps.isSuccess());
                if (ps.isSuccess())
                    break;
                archive.cleanPending();
                System.out.println("retry task");
                ps = archive.pendSnapshot(machine.lastApplied(), 1, getTask(true));
            }
        }
        System.out.println(archive.lastSnapshot().path());
    }

}
