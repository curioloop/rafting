package io.lubricant.consensus.raft.command.admin;

import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.command.RaftMachine;
import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.admin.domain.CtxStatus;
import io.lubricant.consensus.raft.command.admin.domain.Echo;
import io.lubricant.consensus.raft.command.admin.domain.NextTx;
import io.lubricant.consensus.raft.command.admin.domain.OptimisticTx;
import io.lubricant.consensus.raft.command.admin.stm.*;
import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.RaftException;
import io.lubricant.consensus.raft.support.serial.CmdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Administrator implements RaftMachine {

    public static final String ID = "@raft";

    private final Logger logger = LoggerFactory.getLogger(Administrator.class);

    private final Path path;
    private final CmdSerializer serializer = new RocksSerializer();
    private final KVEngine engine = new KVEngine();

    private final ContextManager manager;
    private RaftStub stub;
    private volatile boolean closed;

    public Administrator(Path path, ContextManager manager) throws Exception {
        this.path = Objects.requireNonNull(path);
        this.manager = Objects.requireNonNull(manager);
        if (Files.exists(path)) {
            engine.loadFrom(path.toFile());
        }
        logger.info("Admin resume status: {}", engine.entries());
        for (Map.Entry<String, Version> e : engine.entries()) {
            CtxStatus status = CtxStatus.from(e.getValue().value());
            if (status == CtxStatus.NORMAL) {
                manager.createContext(e.getKey());
            }
        }
    }

    @Override
    public void initialize(RaftContext context) throws Exception {
        stub = new RaftStub(context, null);
    }

    public RaftContext get(String contextId) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        try {
            MVStore mvStore = stub.submit(new Echo()).get();
            Version version = mvStore.get(contextId);
            if (CtxStatus.from(version.value()) == CtxStatus.NORMAL) {
                return manager.getContext(contextId);
            }
            return null;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RaftException)
                throw (RaftException) ex.getCause();
            throw ex;
        }
    }

    public RaftContext open(String contextId, boolean create) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        try {
            STM stm = stub.submit(new NextTx()).get();
            CtxStatus status = CtxStatus.from(stm.get(contextId));
            if (status == CtxStatus.NORMAL) {
                return manager.getContext(contextId);
            }
            if (create) {
                stm.put(contextId, CtxStatus.to(CtxStatus.NORMAL));
                if (stub.submit(new OptimisticTx(stm)).get()) {
                    return manager.getContext(contextId);
                }
            }
            return null;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RaftException)
                throw (RaftException) ex.getCause();
            throw ex;
        }
    }

    public boolean close(String contextId, boolean destroy) throws ExecutionException, InterruptedException {
        if (closed) throw new IllegalStateException("closed");
        try {
            STM stm = stub.submit(new NextTx()).get();
            CtxStatus status = CtxStatus.from(stm.get(contextId));
            if (status != CtxStatus.NORMAL) return true;
            stm.put(contextId, CtxStatus.to(destroy ? CtxStatus.DESTROYED: CtxStatus.SLEEPING));
            return stub.submit(new OptimisticTx(stm)).get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RaftException)
                throw (RaftException) ex.getCause();
            throw ex;
        }
    }

    @Override
    public long lastApplied() {
        return engine.appliedIdx();
    }

    @Override
    public Object apply(Entry entry) throws Exception {
        if (closed) throw new IllegalStateException("closed");

        Command command = serializer.deserialize(entry);
        if (command instanceof Echo) {
            return engine.seenIndex(entry.index());
        }
        if (command instanceof NextTx) {
            return new STM(engine.nextTx(entry.index()), engine);
        }
        if (command instanceof OptimisticTx) {
            OptimisticTx tx = (OptimisticTx) command;
            return engine.commitTx(entry.index(), tx.txId(), tx.modSet(), (store) -> {
                if (tx.modSet().size() > 0) {
                    for (Map.Entry<Revision, String> e : tx.modSet().entrySet()) {
                        String contextId = e.getKey().key();
                        CtxStatus status = CtxStatus.from(e.getValue());
                        CtxStatus current = CtxStatus.from(store.get(contextId).value());
                        if (current != status) {
                            switch (status) {
                                case NORMAL: manager.createContext(contextId); break;
                                case SLEEPING: manager.exitContext(contextId); break;
                                case DESTROYED: manager.destroyContext(contextId); break;
                            }
                            logger.info("Admin change RaftCtx({}) status: {}=>{}", contextId, current, status);
                        }
                    }
                }
            });
        }
        throw new UnsupportedOperationException(command.getClass().getSimpleName());
    }

    @Override
    public Future<Checkpoint> checkpoint(long mustIncludeIndex) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        if (engine.appliedIdx() < mustIncludeIndex) {
            return null;
        }
        KVEngine ng = engine.clone();
        Path temp = Files.createTempFile("kv-store", null);
        engine.dumpTo(temp.toFile());
        Files.copy(temp, path, StandardCopyOption.REPLACE_EXISTING);
        return Promise.completedFuture(new Checkpoint(temp, ng.appliedIdx()) {
            @Override
            public void release() {
                try { Files.deleteIfExists(temp); }
                catch (IOException ignore) {}
            }
        });
    }

    @Override
    public void recover(Checkpoint checkpoint) throws Exception {
        if (closed) throw new IllegalStateException("closed");
        engine.loadFrom(checkpoint.path().toFile());
        engine.dumpTo(path.toFile());
    }

    @Override
    public void close() throws Exception {
        closed = true;
        Path temp = Files.createTempFile("kv-store", null);
        engine.dumpTo(temp.toFile());
        Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
    }

}
