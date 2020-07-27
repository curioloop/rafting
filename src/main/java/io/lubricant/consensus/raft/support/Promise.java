package io.lubricant.consensus.raft.support;


import io.lubricant.consensus.raft.support.anomaly.TimeoutException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * 异步回调通知（消除异步请求的线程阻塞）
 */
@SuppressWarnings("all")
public class Promise<V> extends CompletableFuture<V> {

    private volatile boolean expired;
    private volatile ScheduledFuture timeout;

    public void timeout(long timeout, TimeLimited onTimeout) {
        if (! isDone()) {
            this.timeout = TimeLimited.newTimer(timeout).schedule(() -> {
                this.timeout = null; // reduce footprint
                if (super.completeExceptionally(new TimeoutException())) {
                    expired = true;
                    onTimeout.timeout();
                }
            });
        }
        if (isDone() && this.timeout != null) {
            cancelTimeout();
        }
    }

    @Override
    public boolean complete(V result) {
        return super.complete(result) && cancelTimeout();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return super.completeExceptionally(ex) && cancelTimeout();
    }

    public boolean isExpired() {
        return expired;
    }

    private boolean cancelTimeout() {
        ScheduledFuture timeout = this.timeout;
        if (timeout != null) {
            timeout.cancel(false);
        }
        return true;
    }

}
