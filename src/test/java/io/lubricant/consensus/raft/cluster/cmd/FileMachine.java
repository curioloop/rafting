package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;

import java.io.*;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class FileMachine implements RaftMachine {

    private final Path path;
    private BufferedWriter writer;
    private CmdSerializer serializer;
    private volatile long lastApplied;
    private volatile boolean closed;

    public FileMachine(String path, CmdSerializer serializer) throws IOException {
        this.path = Paths.get(Objects.requireNonNull(path));
        if (Files.notExists(this.path)) {
            Files.createFile(this.path);
        }
        FileReader in = new FileReader(this.path.toFile());
        LineNumberReader reader = new LineNumberReader(in);
        reader.skip(Long.MAX_VALUE);
        lastApplied = reader.getLineNumber() + Long.signum(reader.getLineNumber());
        reader.close();

        /*in = new FileReader(this.path.toFile());
        reader = new LineNumberReader(in);
        String line , last = null;
        while ((line = reader.readLine()) != null) last = line;
        System.out.println(String.format("LAST LINE IS %s (%d) (%d)", last, reader.getLineNumber(), lastApplied));
        if (lastApplied != reader.getLineNumber()) throw new AssertionError();*/

        this.writer = new BufferedWriter(new FileWriter(path, true));
        this.serializer = serializer;
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    private void updateLastApplied(long index) throws IOException {
        if (index != lastApplied + 1)
            throw new IllegalArgumentException("not continue index " + lastApplied + " " + index);
        lastApplied = index;
    }

    @Override
    public Object apply(RaftLog.Entry entry) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        RaftStub.Command command = serializer.deserialize(entry);
        return apply(entry.index(), command);
    }

    public synchronized boolean apply(long index, RaftStub.Command command) throws IOException {

        if (index <= lastApplied) {
            throw new AssertionError(String.format("log entry is already applied %d <= %d", index, lastApplied));
        }

        if (command == null) {
            throw new NullPointerException("command");
        }

        if (command instanceof AppendCommand) {
            AppendCommand append = (AppendCommand) command;
            if (lastApplied > 0) {
                writer.newLine();
            }
            writer.write(index + ":" + append.line());
            writer.flush();
            updateLastApplied(index);
            return true;
        }

        throw new UnsupportedOperationException(command.getClass().getName());
    }

    @Override
    public synchronized Future<Checkpoint> checkpoint(long mustIncludeIndex) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        if (mustIncludeIndex > lastApplied) {
            throw new AssertionError(String.format(
                    "expecting a not existed index %d > %d", mustIncludeIndex, lastApplied));
        }
        Path tempFile = Files.createTempFile(null, null);
        writer.flush();
        Files.copy(path, tempFile.toAbsolutePath(), StandardCopyOption.REPLACE_EXISTING);
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
    public synchronized void recover(Checkpoint checkpoint) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        if (checkpoint.lastIncludeIndex() < lastApplied)
            throw new IllegalArgumentException("invalid snapshot " + lastApplied + " " + checkpoint.lastIncludeIndex() );
        writer.close();
        try {
            validate(checkpoint);
            Files.copy(checkpoint.path(), this.path, StandardCopyOption.REPLACE_EXISTING);
            lastApplied = checkpoint.lastIncludeIndex();
        } finally {
            writer = new BufferedWriter(new FileWriter(this.path.toFile(), true));
        }
    }

    private void validate(Checkpoint checkpoint) throws IOException {
        try (LineNumberReader a = new LineNumberReader(new FileReader(this.path.toFile()));
             LineNumberReader b = new LineNumberReader(new FileReader(checkpoint.path().toFile()))) {
            String lineA, lineB;
            while ((lineA = a.readLine()) != null && (lineB = b.readLine()) != null) {
                if (! lineA.equals(lineB)) {
                    throw new IllegalStateException(String.format("snapshot has diff prefix: %s, %s", lineA, lineB));
                }
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (! closed) {
            closed = true;
            writer.close();
        } else {
            throw new IllegalStateException("close again");
        }
    }
}
