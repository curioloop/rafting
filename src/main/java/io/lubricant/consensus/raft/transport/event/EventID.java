package io.lubricant.consensus.raft.transport.event;

/**
 * 事件ID
 */
public class EventID {

    private final String scope;
    private final NodeID nodeID;

    /**
     * 事件ID
     * @param scope 事件源
     * @param nodeID 事件源所在的节点
     */
    public EventID(String scope, NodeID nodeID) {
        this.scope = scope;
        this.nodeID = nodeID;
    }

    public String scope() {
        return scope;
    }

    public NodeID nodeID() {
        return nodeID;
    }

    @Override
    public int hashCode() {
        return 31 * scope.hashCode() + nodeID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EventID) {
            EventID id = (EventID) obj;
            return scope.equals(id.scope) && nodeID.equals(id.nodeID);
        }
        return false;
    }

    @Override
    public String toString() {
        return scope + '@' + nodeID;
    }
}
