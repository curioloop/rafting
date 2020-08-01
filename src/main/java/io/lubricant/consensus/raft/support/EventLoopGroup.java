package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.EventLoop.ContextEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事件循环线程组
 */
public class EventLoopGroup {

    private static final Logger logger = LoggerFactory.getLogger(EventLoopExecutor.class);

    /**
     * 事件处理线程
     */
    protected static class EventLoopExecutor extends Thread {

        protected volatile boolean isWorking = true;
        protected volatile boolean isRunning = true;
        private final EventLoop eventLoop = new EventLoop(this);

        private EventLoopExecutor(String name) {
            super(RaftThreadGroup.instance(), name);
            setPriority(Thread.MAX_PRIORITY);
        }

        @Override
        public void run() {
            while (isRunning) {
                Runnable task;
                try {
                    task = eventLoop.next();
                } catch (InterruptedException e) {
                    continue;
                }
                if (isRunning) try {
                    task.run();
                } catch (Throwable ex) {
                    logger.error("Uncaught exception in EventLoop({})", getName(), ex);
                }
            }
        }

        public void shutdown(boolean blockade) {
            if (blockade) {
                isWorking = false;
                logger.warn("EventLoop({}) stop accepting new events", getName());
                return;
            }
            isRunning = false;
            interrupt();
            logger.warn("EventLoop({}) discard remaining events {}", getName(), eventLoop.remainEvents());
        }

    }

    private final AtomicInteger counter;
    private final EventLoopExecutor[] executors;

    /**
     * 创建线程池
     * @param size 线程数量
     * @param name 线程名
     */
    public EventLoopGroup(int size, String name) {
        this.counter = new AtomicInteger();
        this.executors = new EventLoopExecutor[size];
        for (int i=0; i<executors.length; i++) {
            executors[i] = new EventLoopExecutor(name + "-" + i);
        }
    }

    public ContextEventLoop next(RaftContext context) {
        int nextExecutor = counter.getAndIncrement() % executors.length;
        return executors[nextExecutor].eventLoop.bind(context);
    }

    public synchronized void start() {
        for (EventLoopExecutor executor : executors) {
            executor.start();
        }
    }

    public synchronized void shutdown(boolean blockade) {
        for (EventLoopExecutor executor : executors) {
            executor.shutdown(blockade);
        }
    }
}
