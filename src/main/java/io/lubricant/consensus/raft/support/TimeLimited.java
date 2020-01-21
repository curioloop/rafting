package io.lubricant.consensus.raft.support;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 超时回调
 */
public interface TimeLimited {

    void timeout();

    /**
     * 定时器
     */
    interface Timer {
        ScheduledFuture<?> schedule(TimeLimited timeLimited);
    }

    ScheduledExecutorService timeoutTimer = new ScheduledThreadPoolExecutor(
            Math.min(6, Runtime.getRuntime().availableProcessors()),
            RaftThreadGroup.instance().newFactory("TimeoutHandler"));

    static Timer newTimer(long timeout) {
        return task -> timeoutTimer.schedule(task::timeout, timeout, TimeUnit.MILLISECONDS);
    }

}