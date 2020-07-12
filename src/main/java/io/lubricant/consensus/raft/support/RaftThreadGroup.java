package io.lubricant.consensus.raft.support;

import io.netty.util.concurrent.FastThreadLocalThread;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程组
 */
public class RaftThreadGroup extends ThreadGroup {

    private final static RaftThreadGroup INSTANCE = new RaftThreadGroup();

    public static RaftThreadGroup instance() {
        return INSTANCE;
    }

    private final static Set<String> namespace = new HashSet<>();
    private static boolean unique(String name, boolean occupy) {
        synchronized (namespace) {
            if (namespace.contains(name))
                throw new IllegalArgumentException("duplicate namespace: " + name);
            if (occupy) namespace.add(name);
            return true;
        }
    }

    private RaftThreadGroup() {
        super("RaftThreadGroup");
    }

    private Thread newThread(String name, Runnable target) {
        return unique(name, true) ? new Thread(this, target, name): null;
    }

    /**
     * 新建线程工厂
     * @param name 线程名
     * @param singleton 是否为单例线程
     * @return
     */
    public ThreadFactory newFactory(String name, boolean singleton) {
        ThreadFactory factory = null;
        if (singleton) {
            if (unique(name, false))
                factory = r -> newThread(name, r);
        } else if(! String.format(name, 0).isEmpty() && unique(name, true)) {
            factory = new ThreadFactory() {
                final AtomicInteger n = name.contains("%d") ? new AtomicInteger(): null;
                public Thread newThread(Runnable r) {
                    return new FastThreadLocalThread(RaftThreadGroup.this, r,
                            n == null ? name: String.format(name, n.incrementAndGet()));
                }
            };
        }
        return factory;
    }

    /**
     * 新建线程工厂
     * @param name 线程名
     * @return
     */
    public ThreadFactory newFactory(String name) {
        return newFactory(name, false);
    }
}
