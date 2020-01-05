package io.lubricant.consensus.raft.transport.rpc;

import io.lubricant.consensus.raft.support.TimeLimited;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 异步请求
 */
public class Async<T> {

    private final Executor exec;
    private final Callable<T> call;
    private final FutureFactory<T> factory;

    /**
     * 产生异步请求
     * @param exec
     * @param call
     * @param factory
     */
    private Async(Executor exec, Callable<T> call, FutureFactory<T> factory) {
        if (factory == null) {
            factory = AsyncFuture::new;
        }
        this.exec = exec;
        this.call = call;
        this.factory = factory;
    }

    /**
     * 注册回调函数并发起请求
     * @param head
     * @param timeout
     * @param callback
     * @return
     */
    public FutureTask<T> on(AsyncHead head, long timeout, AsyncCallback<T> callback) {
        AsyncFuture<T> future = factory.async(call, callback, TimeLimited.newTimer(timeout), head);
        if (! future.isCancelled()) {
            exec.execute(future);
        }
        return future;
    }

    private static final AsyncFuture SENTINEL = new AsyncFuture();

    /**
     * 异步回调
     */
    @FunctionalInterface
    public interface AsyncCallback<T> {
        void on(T result, Throwable error, boolean canceled);
    }

    /**
     * Future工厂
     */
    @FunctionalInterface
    interface FutureFactory<T> {
        AsyncFuture<T> async(Callable<T> call, AsyncCallback<T> callback, TimeLimited.Timer timer, AsyncHead head);
    }

    /**
     * 异步请求头（用于管理多个请求之间的关联关系）
     */
    public static class AsyncHead extends AbstractQueuedSynchronizer {

        protected int tryAcquireShared(int acquires) {
            return getState() - acquires;
        }
        protected boolean tryReleaseShared(int releases) {
            for (;;) {
                int c = getState();
                if (compareAndSetState(c, c + releases))
                    return true;
            }
        }

        private static class Node {
            private final AsyncFuture async;
            private final Node next;
            private Node(AsyncFuture async, Node next) {
                this.async = async;
                this.next = next;
            }
            private void abort() {
                Node n = this;
                while (n != null) {
                    n.async.onAbort();
                    n = n.next;
                }
            }
        }

        private static final Node WASTED;
        private static final AsyncHead ABORTED;
        static {
            ABORTED = new AsyncHead();
            ABORTED.head = WASTED = new Node(SENTINEL, null);
            ABORTED.setState(Integer.MAX_VALUE);
        }

        private Node head;
        private AsyncHead() {}
        private synchronized Node heading(AsyncFuture prev, boolean next) {
            Node node = head;
            if (node == null || node.async != SENTINEL) {
                head = prev == SENTINEL ? WASTED: new Node(prev, node);
            }
            return next ? node: head;
        }

        private void countDown(Node node) {
            releaseShared(1);
            if (node != WASTED && head != WASTED) {
                synchronized (this) {
                    if (head != WASTED) {
                        Node newHead = node.next;
                        for (Node n = head; n != node; n = n.next) {
                            newHead = new Node(n.async, newHead);
                        }
                        head = newHead;
                    }
                }
            }
        }

        public boolean waitRequests(int n) {
            while (! isAborted()) try {
                acquireSharedInterruptibly(n);
                return ! isAborted();
            } catch (InterruptedException e) {
                if (isAborted()) {
                    return false;
                }
            }
            return false;
        }

        public boolean waitRequests(int n, long timeout) throws TimeoutException {
            if (! isAborted()) try {
                boolean acquired = tryAcquireSharedNanos(n, TimeUnit.MILLISECONDS.toNanos(timeout));
                if (! acquired) throw new TimeoutException();
                return ! isAborted();
            } catch (InterruptedException e) {
                if (isAborted()) {
                    return false;
                }
            }
            return false;
        }

        public boolean isAborted() {
            return head != null && head.async == SENTINEL;
        }

        public void abortRequests(Executor... exec) {
            Node next = heading(SENTINEL, true);
            if (next != null) {
                Executor executor = (exec == null || exec.length == 0) ? null: exec[0];
                if (executor == null) {
                    next.abort();
                } else {
                    executor.execute(next::abort);
                }
            }
        }
    }

    /**
     * 异步请求结果
     */
    public static class AsyncFuture<T> extends FutureTask<T> implements TimeLimited {

        private final AsyncHead head;
        private final AsyncHead.Node node;
        private final AsyncCallback<T> callback;
        private final ScheduledFuture<?> schedule;
        private final AtomicReference<Object> reference;

        private AsyncFuture() {
            super(()->null);
            this.head = null;
            this.node = null;
            this.schedule = null;
            this.reference = null;
            this.callback = null;
            cancel(false);
        }

        public AsyncFuture(Callable<T> call, AsyncCallback<T> callback, TimeLimited.Timer timer, AsyncHead head) {
            super(call);
            this.head = head;
            this.callback = Objects.requireNonNull(callback);
            reference = new AtomicReference<>();
            node = head.heading(this, false);
            if (node == AsyncHead.WASTED) {
                schedule = null;
                cancel(true);
            } else {
                schedule = timer.schedule(this);
            }
        }

        protected void set(T response) {
            if (reference.compareAndSet(null, response)) {
                super.set(response);
                if (schedule != null)
                    schedule.cancel(false);
            }
        }

        protected void setException(Throwable error) {
            if (reference.compareAndSet(null, error)) {
                super.setException(error);
                if (schedule != null)
                    schedule.cancel(false);
            }
        }

        public void timeout() {
            if (reference.compareAndSet(null, new TimeoutException())) {
                cancel(true);
            }
        }

        private void onAbort() {
            if (! isCancelled() && reference.get() == null) {
                cancel(true);
                if (schedule != null && ! schedule.isCancelled())
                    schedule.cancel(false);
            }
        }

        protected void done() {
            try {
                if (reference != null) {
                    Object outcome = reference.get();
                    if (outcome instanceof Throwable) {
                        callback.on(null, (Throwable) outcome, false);
                    } else {
                        callback.on((T) outcome, null, isCancelled());
                    }
                }
            } finally {
                if (head != null) {
                    head.countDown(node);
                }
            }
        }

    }

    /**
     * 生成异步请求头
     * @return
     */
    public static AsyncHead head() {
        return new AsyncHead();
    }

    /**
     * 准备发起异步调用
     * @param exec 线程池（同步调用转为异步调用）
     * @param call 调用逻辑
     * @param factory future 工厂
     * @return
     */
    public static <T> Async<T> call(Executor exec, Callable<T> call, FutureFactory<T> factory) {
        return new Async<>(exec, call, factory);
    }


}