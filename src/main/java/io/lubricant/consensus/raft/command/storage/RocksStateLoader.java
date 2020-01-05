package io.lubricant.consensus.raft.command.storage;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.support.RaftConfig;

import java.nio.file.Files;
import java.nio.file.Path;
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
        if (rocksLog == null) {
            rocksLog = new RocksLog(config.logPath().resolve(contextId).toString(), new RocksSerializer());
            logMap.put(contextId, rocksLog);
        }
        return rocksLog;
    }

    @Override
    public synchronized void close() throws Exception {
        logMap.clear();
    }
}
