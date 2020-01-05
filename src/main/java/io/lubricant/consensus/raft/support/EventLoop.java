package io.lubricant.consensus.raft.support;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import io.lubricant.consensus.raft.support.EventLoopGroup.EventLoopExecutor;

/**
 * 事件循环
 */
public class EventLoop implements Executor {

    private final EventLoopExecutor executor; // 每个事件循环绑定一个唯一的处理线程，保存线程安全
    private final LinkedBlockingDeque<Runnable> eventQueue // 线程队列，通过双端队列来提供任务优先级
            = new LinkedBlockingDeque<>(1024 * 1024);

    protected EventLoop(EventLoopExecutor executor) {
        this.executor = executor;
    }

    public boolean inEventLoop() {
        return Thread.currentThread() == executor;
    }

    protected Runnable next() throws InterruptedException {
        return eventQueue.take();
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
}
