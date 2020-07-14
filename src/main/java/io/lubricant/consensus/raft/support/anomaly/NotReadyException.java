package io.lubricant.consensus.raft.support.anomaly;

/**
 * 未就绪异常（当前集群的健康节点数不足时抛出）
 */
public class NotReadyException extends Exception {

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}