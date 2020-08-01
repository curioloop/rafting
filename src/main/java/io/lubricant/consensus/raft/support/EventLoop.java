package io.lubricant.consensus.raft.support;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.LongAdder;

import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.EventLoopGroup.EventLoopExecutor;
import io.lubricant.consensus.raft.support.anomaly.ExistedLoopException;

/**
 * 事件循环
 */
public class EventLoop implements Executor {

    private final static int QUEUE_LIMIT = 500000;
    private final static int QUEUE_BUSY_HINT = 1000;

    private final LongAdder size = new LongAdder();
    private final EventLoopExecutor executor; // 每个事件循环绑定一个唯一的处理线程，保存线程安全
    private final LinkedBlockingDeque<Runnable> eventQueue // 线程队列，通过双端队列来提供任务优先级
            = new LinkedBlockingDeque<>(QUEUE_LIMIT);

    protected EventLoop(EventLoopExecutor executor) {
        this.executor = executor;
    }

    Runnable next() throws InterruptedException {
        Runnable event = eventQueue.take();
        size.decrement();
        return event;
    }

    public ContextEventLoop bind(RaftContext context) {
        return new ContextEventLoop(context);
    }

    /**
     * 与特定上下文绑定的事件循环
     * */
    public class ContextEventLoop {

        private final RaftContext context;

        private ContextEventLoop(RaftContext context) {
            this.context = context;
        }

        public void execute(Runnable event, boolean urgent) {
            if (context.stillRunning()) {
                EventLoop.this.execute(() -> { if (context.stillRunning()) event.run(); }, urgent);
            }
        }

        public void execute(Runnable event) {
            if (context.stillRunning()) {
                EventLoop.this.execute(() -> { if (context.stillRunning()) event.run(); });
            }
        }

        public void enforce(Runnable event) {
            EventLoop.this.execute(event);
        }

        public boolean inEventLoop() {
            return EventLoop.this.inEventLoop();
        }

        public boolean isAvailable() {
            return EventLoop.this.isAvailable();
        }

        public boolean isRunning() {
            return EventLoop.this.isRunning();
        }

        public boolean isBusy() {
            return EventLoop.this.isBusy();
        }
    }

    /**
     * 异步处理事件
     * @param event 事件处理逻辑
     * @param urgent 是否紧急（紧急事件将放在队列头部，优先处理）
     */
    public void execute(Runnable event, boolean urgent) {
        if (executor.isRunning) {
            if (inEventLoop()) {
                event.run();
            } else {
                if (urgent)
                    eventQueue.addFirst(event);
                else
                    eventQueue.addLast(event);
                size.increment();
            }
        } else {
            throw new ExistedLoopException();
        }
    }

    /**
     * 异步处理事件
     * @param event 事件处理逻辑
     */
    @Override
    public void execute(Runnable event) {
        execute(event, false);
    }

    /**
     * 是否处于事件循环中
     */
    boolean inEventLoop() {
        return Thread.currentThread() == executor;
    }

    /**
     * 事件循环是否接受新作业
     */
    boolean isAvailable() {
        return executor.isWorking && ! isBusy();
    }

    /**
     * 事件循环是否正在处理作业
     */
    boolean isRunning() {
        return executor.isRunning;
    }

    /**
     * 队列是否繁忙
     */
    boolean isBusy() {
        return QUEUE_LIMIT - size.longValue() < QUEUE_BUSY_HINT;
    }

    /**
     * 队列中的积压的事件数量
     */
    int remainEvents() {
        return eventQueue.size();
    }

}
