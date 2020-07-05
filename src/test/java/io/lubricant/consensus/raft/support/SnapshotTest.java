package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.cluster.cmd.AppendCommand;
import io.lubricant.consensus.raft.cluster.cmd.FileMachine;
import io.lubricant.consensus.raft.command.MaintainAgreement;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
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
        archive = new SnapshotArchive(Paths.get("/data0"), "snapshot-archive-test", 3);
        machine = new FileMachine(dir.resolve("machine").toString(), new RocksSerializer());
        machine.apply(1, new AppendCommand("1"));
        machine.apply(2, new AppendCommand("2"));
        machine.apply(3, new AppendCommand("3"));
    }

    private PendingTask<SnapshotArchive.Snapshot> getTask( boolean success, long delay) throws Exception {
        return getTask(0, 1, success, delay);
    }

    private PendingTask<SnapshotArchive.Snapshot> getTask(int incr, int term, boolean success, long delay) throws Exception {
        while (incr-- > 0) {
            long i = machine.lastApplied() + 1;
            machine.apply(i, new AppendCommand("" + i));
        }
        Future<RaftMachine.Checkpoint> future = machine.checkpoint(machine.lastApplied());
        return new PendingTask<SnapshotArchive.Snapshot>() {
            @Override
            protected void perform() {
                TimeLimited.newTimer(delay).schedule(()->{
                    RaftMachine.Checkpoint checkpoint = null;
                    if (! success) {
                        setException(new RejectedExecutionException());
                        return;
                    }
                    try {
                        checkpoint = future.get();
                        set(new SnapshotArchive.Snapshot(checkpoint.path(), checkpoint.lastIncludeIndex(), term));
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
        SnapshotArchive.PendingSnapshot ps = archive.pendSnapshot(machine.lastApplied(), 1, getTask(false, 5000));
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
                ps = archive.pendSnapshot(machine.lastApplied(), 1, getTask(true, 5000));
            }
        }
        System.out.println(archive.lastSnapshot().path());
    }

    @Test
    public void testMulti() throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int index = 3, term = 1;
        int count = 25;
        int prevIndex = index, snapTerm = term;
        SnapshotArchive.PendingSnapshot ps = archive.pendSnapshot(prevIndex, snapTerm, getTask(0, snapTerm, false, 500));
        Assert.assertEquals(ps, archive.pendSnapshot(machine.lastApplied(), 1, null));
        while (true) {
            index = (int) machine.lastApplied() + 1;
            machine.apply(index, new AppendCommand("" + index));
            term+=rand.nextInt(5);

            if (rand.nextInt(3) == 0) {
                int newIndex = (int) machine.lastApplied();
                int newTerm = Math.max(snapTerm, term + rand.nextInt(1));
                SnapshotArchive.PendingSnapshot newPs = archive.pendSnapshot(newIndex, newTerm, getTask(rand.nextInt(3), newTerm, rand.nextBoolean(), 500));
                if (newIndex == prevIndex && newTerm <= snapTerm)
                    Assert.assertEquals(
                            String.format("(%d:%d) <= (%d:%d)", newIndex, newTerm, prevIndex, snapTerm),
                            newPs, ps);
                else
                    Assert.assertNotEquals(
                            String.format("(%d:%d) > (%d:%d)", newIndex, newTerm, prevIndex, snapTerm),
                            newPs, ps);
                ps = newPs;
                prevIndex = newIndex;
                snapTerm = newTerm;
            }

            if (ps.isPending()) {
                Thread.sleep(50);
            } else{
                System.out.println("task finish : " + ps.isSuccess());
                if (ps.isSuccess()) {
                    SnapshotArchive.Snapshot snap = archive.lastSnapshot();
                    System.out.println(String.format("(%d:%d) [%d:%d] %s", prevIndex, snapTerm, snap.lastIncludeIndex(), snap.lastIncludeTerm(), snap.path()));
                    if (count-- <= 0) break;
                }
                archive.cleanPending();
                prevIndex = (int) machine.lastApplied();
                snapTerm = Math.max(snapTerm, term + rand.nextInt(5));
                ps = archive.pendSnapshot(prevIndex, snapTerm, getTask(0, snapTerm, rand.nextBoolean(), 500));
            }
        }

    }

    @Test
    public void testArg() throws InterruptedException {
        MaintainAgreement ma = new MaintainAgreement(
                1000, 5000, 10000,
                5, 5);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("hh:mm:ss");
        System.out.println(fmt.format(LocalDateTime.now()));

        long minLogIndex = 0;
        long lastAppliedIndex = 0;
        while (true) {

            if (ma.needMaintain(lastAppliedIndex)) {
                boolean success = random.nextBoolean();
                ma.triggerMaintenance();
                Thread.sleep(100); // Thread.sleep(15000);
                ma.finishMaintenance(success);
                System.out.println("maintain "+ success +" : " + fmt.format(LocalDateTime.now()));
            } else {
                ma.increaseCommand();
                ma.minimalLogIndex(++minLogIndex);
            }

            if (ma.needCompact()) {
                boolean success = random.nextBoolean();
                ma.triggerCompaction();
                Thread.sleep(100);  // Thread.sleep(15000);
                ma.finishCompaction(success);
                System.out.println("compact "+ success +" : " +  fmt.format(LocalDateTime.now()));
            } else {
                minLogIndex++;
                ma.snapshotIncludeEntry(minLogIndex, 1);
            }
        }

    }
}
