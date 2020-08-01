package io.lubricant.consensus.raft.support.anomaly;

import java.util.concurrent.TimeUnit;

/**
 * 命令重试异常
 */
public class RetryCommandException extends Exception {

    private final long delayMills;

    public RetryCommandException(long delay, TimeUnit unit) {
        this.delayMills = unit.toMillis(delay);
        if (delayMills <= 0) {
            throw new IllegalArgumentException("illegal retry interval " + delayMills + " ms");
        }
    }

    public long delayMills() {
        return delayMills;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
