package io.lubricant.consensus.raft.support;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * 异步回调通知（消除异步请求的线程阻塞）
 */
@SuppressWarnings("all")
public class Promise<V> extends CompletableFuture<V> {

    private volatile ScheduledFuture timeout;

    public void timeout(long timeout, TimeLimited onTimeout) {
        this.timeout = TimeLimited.newTimer(timeout).schedule(() -> {
            this.timeout = null; // reduce footprint
            onTimeout.timeout();
        });
    }

    @Override
    public boolean complete(V result) {
        return super.complete(result) && triggerTimeout();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return super.completeExceptionally(ex) && triggerTimeout();
    }

    private boolean triggerTimeout() {
        ScheduledFuture timeout = this.timeout;
        if (timeout != null) {
            timeout.cancel(false);
        }
        return true;
    }

}
