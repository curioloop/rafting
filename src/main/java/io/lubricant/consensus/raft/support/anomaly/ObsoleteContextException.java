package io.lubricant.consensus.raft.support.anomaly;

/**
 * 上下文废弃异常（当前上下文已经失效）
 */
public class ObsoleteContextException extends Exception {

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
