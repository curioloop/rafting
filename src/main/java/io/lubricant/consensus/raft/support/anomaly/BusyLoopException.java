package io.lubricant.consensus.raft.support.anomaly;


import io.lubricant.consensus.raft.support.RaftException;

/**
 * 业务繁忙异常（当前队列消息积压过多时抛出）
 */
public class BusyLoopException extends RaftException {
}
