package io.lubricant.consensus.raft.support.anomaly;


/**
 * 队列退出异常（当前队列已经关闭抛出）
 */
public class ExistedLoopException extends RuntimeException {

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
