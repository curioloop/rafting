package io.lubricant.consensus.raft.support.anomaly;


import io.lubricant.consensus.raft.support.RaftException;

/**
 * 队列退出异常（当前队列已经关闭抛出）
 */
public class ExistedLoopException extends RaftException {
}
