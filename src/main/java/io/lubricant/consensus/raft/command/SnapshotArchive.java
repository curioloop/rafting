package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.command.RaftLog.EntryKey;
import io.lubricant.consensus.raft.command.RaftMachine.Checkpoint;
import io.lubricant.consensus.raft.support.PendingTask;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.Predicate;

/**
 * 快照管理
 */
public class SnapshotArchive {

    private static final String SNAPSHOT_PREFIX = "snapshot_";
    private static final String TEMP_SUFFIX = "_tmp";
    private static final String INDEX_TERM_PAT = "([0-9A-Fa-f]+?)_([0-9A-Fa-f]+?)$";

    public static boolean supportAtomicMove;
    static {
        Path tmpSrc, tmpDst;
        try {
            tmpSrc = Files.createTempFile(null, null);
            tmpDst = Files.createTempFile(null, null);
            boolean support;
            try {
                Files.copy(tmpSrc, tmpDst, StandardCopyOption.ATOMIC_MOVE);
                support = true;
            } catch (UnsupportedOperationException e) {
                support = false;
            }
            supportAtomicMove = support;
            Files.deleteIfExists(tmpSrc);
            Files.deleteIfExists(tmpDst);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private final int snapLimit;
    private final Path ctxSnapDir;
    private final Predicate<Path> snapFilter;
    private volatile PendingSnapshot pendingSnapshot;

    /**
     * 快照信息
     * 记录了最后一个日志条目的索引与任期
     */
    public static class Snapshot extends Checkpoint {
        private final long term;
        public Snapshot(Path path, long index, long term) {
            super(path, index);
            this.term = term;
        }
        public long lastIncludeTerm() { return term; }
    }

    /**
     * 快照任务
     * */
    public class PendingSnapshot {

        private final long index, term;
        private final PendingTask<Snapshot> task;
        private volatile Snapshot snapshot;
        private volatile boolean isPending = true;

        PendingSnapshot(long index, long term, PendingTask<Snapshot> task) {
            this.index = index;
            this.term = term;
            this.task = task;
            task.perform((() -> {
                try {
                    if (task.isSuccess()) {
                        Snapshot snap = task.result();
                        snapshotPath(snap.lastIncludeIndex(), snap.lastIncludeTerm());
                        saveCheckpoint(snap, snap.lastIncludeTerm(), true);
                        snapshot = lastSnapshot();
                    }
                } finally {
                    isPending = false;
                }
                return null;
            }));
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public boolean abort() {
            return ! task.isDone() && task.cancel(true);
        }

        public boolean isSuccess() {
            return ! isPending && task.isSuccess();
        }

        public boolean isPending() {
            return isPending;
        }

        public boolean isExpired(long index, long term) {
            return this.index < index || this.index == index && this.term < term;
        }
    }

    public SnapshotArchive(Path snapshotDir, int limit) throws IOException {
        snapLimit = limit;
        ctxSnapDir = snapshotDir;
        if (! Files.exists(ctxSnapDir)) {
            Files.createDirectory(ctxSnapDir);
        }
        if (! Files.isDirectory(ctxSnapDir)) {
            throw new IllegalArgumentException(ctxSnapDir + " is not directory");
        }
        if (! Files.isWritable(ctxSnapDir)) {
            throw new IllegalArgumentException(ctxSnapDir + " is not writable");
        }

        PathMatcher snapMatcher = ctxSnapDir.getFileSystem().getPathMatcher("regex:" + SNAPSHOT_PREFIX + INDEX_TERM_PAT);
        PathMatcher tempMatcher = ctxSnapDir.getFileSystem().getPathMatcher("glob:" + SNAPSHOT_PREFIX + "*" + TEMP_SUFFIX);
        this.snapFilter = path -> snapMatcher.matches(path.getFileName());

        Iterator<Path> iterator = Files.walk(ctxSnapDir, 1)
                .filter(path -> tempMatcher.matches(path.getFileName())).iterator();
        while (iterator.hasNext()) {
            Files.delete(iterator.next());
        }
    }

    private Path snapshotPath(long index, long term) {
        return ctxSnapDir.resolve(SNAPSHOT_PREFIX + Long.toHexString(index) + "_" + Long.toHexString(term));
    }

    private synchronized boolean saveCheckpoint(Checkpoint checkpoint, long term, boolean pending) throws IOException {
        try {
            if (! Files.isRegularFile(checkpoint.path())){
                throw new AssertionError(checkpoint.path() + " is not regular file");
            }
            Snapshot snapshot = lastSnapshot();
            if (snapshot != null) {
                if (snapshot.lastIncludeIndex() > checkpoint.lastIncludeIndex()) {
                    if (! pending) {
                        throw new AssertionError(String.format("expired checkpoint: %s(%d) < %s(%d)",
                                checkpoint.path(), checkpoint.lastIncludeIndex(),
                                snapshot.path(), snapshot.lastIncludeIndex()));

                    }
                    return false; // pending snapshot expired
                } else if (snapshot.lastIncludeIndex() == checkpoint.lastIncludeIndex()) {
                    if (snapshot.lastIncludeTerm() != term) {
                        throw new AssertionError(String.format("asymmetric commit: %s(%d-%d) != %s(%d-%d)",
                                checkpoint.path(), checkpoint.lastIncludeIndex(), term,
                                snapshot.path(), snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm()));
                    }
                    return false; // snapshot existed
                } else if (term < snapshot.lastIncludeTerm()) {
                    throw new AssertionError(String.format("term rollback: %s(%d-%d) < %s(%d-%d)",
                            checkpoint.path(), checkpoint.lastIncludeIndex(), term,
                            snapshot.path(), snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm()));
                }
            }

            StandardCopyOption[] options = supportAtomicMove ?
                    new StandardCopyOption[]{StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE}:
                    new StandardCopyOption[]{StandardCopyOption.REPLACE_EXISTING};

            Path path = snapshotPath(checkpoint.lastIncludeIndex(), term);
            if (pending) {
                Files.move(checkpoint.path(), path, options);
            } else {
                Files.copy(checkpoint.path(), path, options);
            }
            clean(snapLimit);
            return true;
        } finally {
            checkpoint.release();
        }
    }

    public Snapshot lastSnapshot() throws IOException {
        Map.Entry<EntryKey, Path> entry = allSnapshot().lastEntry();
        if (entry != null) {
            EntryKey key = entry.getKey();
            return new Snapshot(entry.getValue(), key.index(), key.term());
        }
        return null;
    }

    public boolean takeSnapshot(Checkpoint checkpoint, long term) throws IOException {
        return saveCheckpoint(checkpoint, term, false);
    }

    public synchronized PendingSnapshot pendSnapshot(long index, long term, PendingTask<Snapshot> task) {
        if (pendingSnapshot == null) {
            if (task == null) return null;
            pendingSnapshot = new PendingSnapshot(index, term, task);
        } else if (pendingSnapshot.isExpired(index, term)) {
            Snapshot snapshot = pendingSnapshot.snapshot();
            boolean isExpired = snapshot == null || snapshot.lastIncludeIndex() < index ||
                    snapshot.lastIncludeIndex() == index && snapshot.lastIncludeTerm() < term;
            if (isExpired) {
                pendingSnapshot.abort();
                pendingSnapshot = new PendingSnapshot(index, term, task);
            }
        }
        return pendingSnapshot;
    }

    public synchronized void cleanPending() {
        if (pendingSnapshot != null) {
            pendingSnapshot.abort();
            pendingSnapshot = null;
        }
    }

    private void clean(int limit) throws IOException {
        if (limit > 0) {
            TreeMap<EntryKey, Path> paths = allSnapshot();
            while (paths.size() > limit) {
                Path path = paths.pollFirstEntry().getValue();
                Files.delete(path);
            }
        }
    }

    private TreeMap<EntryKey, Path> allSnapshot() throws IOException {
        // order by [index, term] asc
        TreeMap<EntryKey, Path> paths = new TreeMap<>();
        Files.walk(ctxSnapDir, 1)
                .filter(Files::isRegularFile).filter(snapFilter)
                .forEach(path -> {
                    String[] split = path.getFileName().toString().split("_");
                    long index = Long.parseLong(split[1], 16);
                    long term = Long.parseLong(split[2], 16);
                    paths.put(new EntryKey(index, term), path);
                });
        return paths;
    }

}
