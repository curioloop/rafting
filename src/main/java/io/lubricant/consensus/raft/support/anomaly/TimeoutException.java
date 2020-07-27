package io.lubricant.consensus.raft.support.anomaly;

/**
 * 超时异常
 */
public class TimeoutException extends java.util.concurrent.TimeoutException {

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
