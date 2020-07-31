package io.lubricant.consensus.raft.command.admin;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.admin.stm.KVEngine;
import io.lubricant.consensus.raft.command.admin.stm.MVStore;
import io.lubricant.consensus.raft.command.admin.stm.Revision;
import io.lubricant.consensus.raft.command.admin.stm.STM;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class Administrator implements RaftMachine {

    private final Path path;
    private final CmdSerializer serializer;
    private final KVEngine engine = new KVEngine();
    private RaftStub stub;

    static class Echo implements Command<MVStore> {}

    static class NextTx implements Command<STM> {}

    static class OptimisticTx implements Command<Boolean> {
        private final long txId;
        private final Map<Revision, String> modSet;
        OptimisticTx(long txId, Map<Revision, String> modSet) {
            this.txId = txId;
            this.modSet = modSet;
        }
    }

    public Administrator(String path, CmdSerializer serializer) {
        this.path = Paths.get(Objects.requireNonNull(path));
        this.serializer = serializer;
    }

    public void init(RaftStub stub) throws IOException, SerializeException {
        this.stub = stub;
        if (Files.exists(path)) {
            engine.loadFrom(path.toFile());
        }
    }

    public Promise<MVStore> store() {
        return stub.submit(new Echo());
    }

    public Promise<STM> nextTx() {
        return stub.submit(new NextTx());
    }

    public Promise<Boolean> commitTx(STM stm) {
        return stub.submit(new OptimisticTx(stm.txId(), stm.mod()));
    }

    @Override
    public long lastApplied() {
        return engine.appliedIdx();
    }

    @Override
    public Object apply(Entry entry) throws Exception {
        Command command = serializer.deserialize(entry);
        if (command instanceof Echo) {
            return engine.seenIndex(entry.index());
        }
        if (command instanceof NextTx) {
            return new STM(engine.nextTx(entry.index()), engine);
        }
        if (command instanceof OptimisticTx) {
            OptimisticTx tx = (OptimisticTx) command;
            return engine.commitTx(entry.index(), tx.txId, tx.modSet);
        }
        throw new UnsupportedOperationException(command.getClass().getSimpleName());
    }

    @Override
    public Future<Checkpoint> checkpoint(long mustIncludeIndex) throws Exception {
        if (engine.appliedIdx() < mustIncludeIndex) {
            return null;
        }
        KVEngine ng = engine.clone();
        Supplier<Checkpoint> dump = () -> {
            try {
                Path temp = Files.createTempFile("kv-store", null);
                ng.dumpTo(temp.toFile());
                return new Checkpoint(temp, ng.appliedIdx()) {
                    @Override
                    public void release() {
                        try {
                            Files.deleteIfExists(temp);
                        } catch (IOException ignore) {}
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return Promise.supplyAsync(dump);
    }

    @Override
    public void recover(Checkpoint checkpoint) throws Exception {
        engine.loadFrom(checkpoint.path().toFile());
    }

    @Override
    public void close() throws Exception {
        Path temp = Files.createTempFile("kv-store", null);
        engine.dumpTo(temp.toFile());
        Files.move(temp, path);
    }

}
