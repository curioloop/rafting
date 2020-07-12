package io.lubricant.consensus.raft.support;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * 异步任务
 */
@SuppressWarnings("all")
public abstract class PendingTask<T> extends FutureTask<T> {

    private static final sun.misc.Unsafe UNSAFE;
    static {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private static final long stateOffset;
    private static final long performedOffset;
    static {
        try {
            stateOffset = UNSAFE.objectFieldOffset(FutureTask.class.getDeclaredField("state"));
            performedOffset = UNSAFE.objectFieldOffset(PendingTask.class.getDeclaredField("performed"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public interface Trailer<V> {
        V after() throws Exception;
    }

    private static final int NEW = 0;
    private static final int PENDING = 1;
    private static final int PERFORMED = 2;

    private static final Callable NULL = () -> {throw new NullPointerException();};

    private volatile int performed = 0;
    private Trailer trailer;
    private Promise promise;

    public PendingTask() {
        super(NULL);
    }

    public <V> Future<V> perform(Trailer... trailer) {
        if (UNSAFE.compareAndSwapInt(this, performedOffset, NEW, PENDING)) {
            if (trailer != null && trailer.length > 0) {
                promise = new Promise<>();
                this.trailer = trailer[0];
            }
            perform();
            return promise;
        }
        return null;
    }

    protected abstract void perform();

    @Override
    public void run() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void done() {
        // maintain consistent behavior patterns with FutureTask
        // only guarantee the visibility for the same thread
        UNSAFE.putOrderedInt(this, performedOffset, PERFORMED);
        if (trailer != null) {
            try {
                promise.complete(trailer.after());
            } catch (Exception e) {
                promise.completeExceptionally(e);
            }
        }
    }

    public boolean isPending() {
        return performed <= PENDING;
    }

    @Override
    public boolean isDone() {
        return performed == PERFORMED;
    }

    public boolean isSuccess() {
        return UNSAFE.getIntVolatile(this, stateOffset) == 2;
    }

    public T result() {
        try {
            return get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void join() throws Exception {
        get();
    }
}
