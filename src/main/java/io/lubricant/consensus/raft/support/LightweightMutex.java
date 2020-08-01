package io.lubricant.consensus.raft.support;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 轻量条件（不可重入）
 * */
public class LightweightMutex extends AtomicReference<Object> {

    private static final Object INVALID = new Object();
    private final ReentrantLock LOCKED = new ReentrantLock();
    private final Condition condition = LOCKED.newCondition();

    public boolean tryLock() {
        if (compareAndSet(null, LOCKED))
            return true;
        Object o = get();
        if (o == INVALID) return false;
        throw new AssertionError("encountered an unexpected status : " + o);
    }

    public boolean unlock() {
        while (true) {
            if (compareAndSet(LOCKED, null))
                return true;
            Object o = get();
            if (o == INVALID) {
                LOCKED.lock();
                try { condition.signalAll(); }
                finally { LOCKED.unlock(); }
                return false;
            }
            throw new AssertionError("encountered an unexpected status : " + o);
        }
    }

    public void await() throws InterruptedException {
        while (get() != INVALID) {
            if (compareAndSet(LOCKED, INVALID)) {
                LOCKED.lock();
                try { condition.await(); }
                finally { LOCKED.unlock(); }
            } else if (compareAndSet(null, INVALID)) {
                return;
            }
        }
    }

}
