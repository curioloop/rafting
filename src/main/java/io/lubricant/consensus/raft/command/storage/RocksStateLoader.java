package io.lubricant.consensus.raft.command.storage;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.support.RaftConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksStateLoader implements StateLoader {

    private RaftConfig config;
    private Map<String, RocksLog> logMap;

    public RocksStateLoader(RaftConfig config) throws Exception {
        Path path = config.logPath();
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        this.config = config;
        logMap = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized RaftLog restore(String contextId, boolean create) throws Exception {
        RocksLog rocksLog = logMap.get(contextId);
        if (rocksLog == null && create) {
            rocksLog = new RocksLog(config.logPath().resolve(contextId).toString(), this, new RocksSerializer());
            logMap.put(contextId, rocksLog);
        }
        return rocksLog;
    }

    synchronized void close(RocksLog log) {
        log.db.close();
        String contextId = log.path.toFile().getName();
        if (logMap.remove(contextId) != log) {
            throw new AssertionError("rock log instance should not changed");
        }
    }

    synchronized void destroy(RocksLog log) throws IOException {
        String contextId = log.path.toFile().getName();
        if (logMap.containsKey(contextId)) {
            throw new IllegalStateException("reopen");
        }
        Path rocksPath = log.path;
        if (rocksPath != null && Files.exists(rocksPath)) {
            Iterator<Path> it = Files.list(rocksPath).iterator();
            while (it.hasNext()) Files.delete(it.next());
            Files.delete(rocksPath);
        }
    }

    @Override
    public synchronized void close() throws Exception {
        for (RocksLog log : logMap.values().toArray(new RocksLog[0])) {
            log.close();
        }
    }
}
