package io.lubricant.consensus.raft.transport.event;

import io.lubricant.consensus.raft.transport.RaftCluster;

/**
 * 节点 ID
 */
public class NodeID implements RaftCluster.ID {

    private final String hostname;
    private final int port;

    /**
     * 节点 ID
     * @param host 主机名或 IP
     * @param port 端口
     */
    public NodeID(String host, int port) {
        this.hostname = host;
        this.port = port;
    }

    public String hostname() {
        return hostname;
    }

    public int port() {
        return port;
    }

    @Override
    public int hashCode() {
        return 31 * hostname.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NodeID) {
            NodeID id = (NodeID) obj;
            return hostname.equals(id.hostname) && port == id.port;
        }
        return false;
    }

    @Override
    public String toString() {
        return hostname + ":" + port;
    }

    public static NodeID fromString(String data) {
        String[] split = data.split(":");
        if (split.length == 2) {
            try {
                return new NodeID(split[0], Integer.parseInt(split[1]));
            } catch (Exception e) {
                throw new IllegalArgumentException(data, e);
            }
        }
        throw new IllegalArgumentException(data);
    }
}
