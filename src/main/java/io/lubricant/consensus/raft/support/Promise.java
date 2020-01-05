package io.lubricant.consensus.raft.support;


import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 异步回调通知（消除异步请求的线程阻塞）
 */
@SuppressWarnings("all")
public class Promise<V> implements Callable<V> {

    final static AtomicReferenceFieldUpdater<Promise, Object>
            resultUpdater = AtomicReferenceFieldUpdater.newUpdater(Promise.class, Object.class, "result");

    private FutureTask<V> future;
    private volatile Object result;
    private volatile ScheduledFuture timeout;

    @Override
    public V call() throws Exception {
        if (result instanceof Exception) {
            throw ((Exception) result);
        }
        return (V) result;
    }

    public void timeout(long timeout, TimeLimited onTimeout) {
        TimeLimited clean = () -> { this.timeout = null; };
        this.timeout = TimeLimited.newTimer(timeout).schedule(clean.join(onTimeout));
    }

    public void future(FutureTask<V> task) {
        this.future = task;
    }

    public void complete(Object result) {
        if (resultUpdater.compareAndSet(this, null, result)) {
            future.run();
            ScheduledFuture timeout = this.timeout;
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }

    public void error(Exception cause) {
        if (resultUpdater.compareAndSet(this, null, cause)) {
            future.run();
            ScheduledFuture timeout = this.timeout;
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }

}
