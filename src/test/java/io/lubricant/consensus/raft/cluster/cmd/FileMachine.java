package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.RaftClient;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;

import java.io.*;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class FileMachine implements RaftMachine {

    private final String path;
    private RandomAccessFile meta;
    private BufferedWriter writer;
    private CmdSerializer serializer;
    private volatile long lastApplied;

    public FileMachine(String path, CmdSerializer serializer) throws IOException {
        this.path = Objects.requireNonNull(path);
        this.meta = new RandomAccessFile(path + "-meta", "rw");
        this.writer = new BufferedWriter(new FileWriter(path, true));
        this.serializer = serializer;
        if (meta.length() > 0) {
            lastApplied = meta.readLong();
        }
    }

    private void updateLastApplied(long index) throws IOException {
        lastApplied = index;
        meta.seek(0);
        meta.writeLong(index);
        meta.getFD().sync();
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    @Override
    public Object apply(RaftLog.Entry entry) throws Exception {
        RaftClient.Command command = serializer.deserialize(entry);
        return apply(entry.index(), command);
    }

    public boolean apply(long index, RaftClient.Command command) throws IOException {

        if (index <= lastApplied) {
            throw new AssertionError(String.format("log entry is already applied %d <= %d", index, lastApplied));
        }

        updateLastApplied(index); // no rollback

        if (command == null) {
            throw new NullPointerException("command");
        }

        if (command instanceof AppendCommand) {
            AppendCommand append = (AppendCommand) command;
            writer.newLine();
            writer.write(append.line());
            writer.flush();
            return true;
        }

        throw new UnsupportedOperationException(command.getClass().getName());
    }

    @Override
    public Future<Checkpoint> checkpoint(long mustIncludeIndex) throws Exception {
        if (mustIncludeIndex > lastApplied) {
            throw new AssertionError(String.format(
                    "expecting a not existed index %d > %d", mustIncludeIndex, lastApplied));
        }
        Path tempFile = Files.createTempFile(null, null);
        writer.flush();
        Files.copy(Paths.get(path), tempFile.toAbsolutePath(), StandardCopyOption.REPLACE_EXISTING);
        return CompletableFuture.completedFuture(new Checkpoint(tempFile, lastApplied){
            @Override
            public void release() {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {}
            }
        });
    }

    @Override
    public void recover(Checkpoint checkpoint) throws Exception {
        writer.close();
        try {
            Files.copy(checkpoint.path(), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
            updateLastApplied(checkpoint.lastIncludeIndex());
        } finally {
            writer = new BufferedWriter(new FileWriter(path, true));
        }
    }

    @Override
    public void close() throws Exception {
        meta.close();
        writer.close();
    }
}
