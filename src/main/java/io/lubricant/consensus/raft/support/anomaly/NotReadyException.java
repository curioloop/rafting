package io.lubricant.consensus.raft.support.anomaly;

import io.lubricant.consensus.raft.support.RaftException;

/**
 * 未就绪异常（当前集群的健康节点数不足时抛出）
 */
public class NotReadyException extends RaftException {
}