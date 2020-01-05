package io.lubricant.consensus.raft.cluster.cmd;

import io.lubricant.consensus.raft.command.RaftClient;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;

import java.io.*;

class FileMachine implements RaftMachine {

    private RandomAccessFile meta;
    private BufferedWriter writer;
    private CmdSerializer serializer;
    private volatile long lastApplied;

    public FileMachine(String path, CmdSerializer serializer) throws IOException {
        this.meta = new RandomAccessFile(path + "-meta", "rw");
        this.writer = new BufferedWriter(new FileWriter(path, true));
        this.serializer = serializer;
        if (meta.length() > 0) {
            lastApplied = meta.readLong();
        }
    }

    @Override
    public long lastApplied() {
        return lastApplied;
    }

    @Override
    public Object apply(RaftLog.Entry entry) throws Exception {
        if (entry.index() <= lastApplied) {
            throw new AssertionError("log entry is already applied");
        }

        lastApplied = entry.index(); // no rollback

        RaftClient.Command command = serializer.deserialize(entry);
        if (command == null) {
            throw new NullPointerException("command");
        }

        if (command instanceof AppendCommand) {
            AppendCommand append = (AppendCommand) command;
            writer.newLine();
            writer.write(append.line());
            writer.flush();
            meta.seek(0);
            meta.writeLong(entry.index());
            meta.getFD().sync();
            return true;
        }

        throw new UnsupportedOperationException(command.getClass().getName());
    }

    @Override
    public void close() throws Exception {
        meta.close();
        writer.close();
    }
}
