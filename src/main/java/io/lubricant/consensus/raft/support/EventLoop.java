package io.lubricant.consensus.raft.support;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.LongAdder;

import io.lubricant.consensus.raft.support.EventLoopGroup.EventLoopExecutor;

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

    public boolean inEventLoop() {
        return Thread.currentThread() == executor;
    }

    protected Runnable next() throws InterruptedException {
        Runnable event = eventQueue.take();
        size.decrement();
        return event;
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
            throw new IllegalStateException("EventLoop is shutdown");
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
     * 队列是否繁忙
     */
    public boolean isBusy() {
        return QUEUE_LIMIT - size.longValue() < QUEUE_BUSY_HINT;
    }

    /**
     * 队列中的积压的事件数量
     */
    int remainEvents() {
        return eventQueue.size();
    }

}
