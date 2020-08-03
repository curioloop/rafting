package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.support.anomaly.WaitTimeoutException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;

/**
 * 异步回调通知（消除异步请求的线程阻塞）
 */
@SuppressWarnings("all")
public class Promise<V> extends CompletableFuture<V> {

    private static class ExceptionWrapper extends CompletionException {
        private ExceptionWrapper(Throwable ex) { super(ex); }
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final ExceptionWrapper TIMEOUT = new ExceptionWrapper(new WaitTimeoutException());

    private final ScheduledFuture timeout;

    public Promise() { timeout = null; }

    public Promise(long timeout) {
        this.timeout = TimeLimited.newTimer(timeout)
                .schedule(() -> super.completeExceptionally(TIMEOUT));
    }

    public CompletableFuture<V> whenTimeout(TimeLimited action) {
        return exceptionally(e -> {if (e == TIMEOUT) action.timeout(); return null;} );
    }

    public boolean finish() {
        return complete(null);
    }

    @Override
    public boolean complete(V result) {
        return super.complete(result) && cancelTimeout();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return super.completeExceptionally(new ExceptionWrapper(ex)) && cancelTimeout();
    }

    public boolean isExpired() {
        if (isCompletedExceptionally()) {
            try { getNow(null); }
            catch (Exception e) { return e == TIMEOUT; }
        }
        return false;
    }

    private boolean cancelTimeout() {
        ScheduledFuture timeout = this.timeout;
        if (timeout != null) {
            timeout.cancel(false);
        }
        return true;
    }
}
