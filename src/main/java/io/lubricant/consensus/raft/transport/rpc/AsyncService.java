package io.lubricant.consensus.raft.transport.rpc;

import io.lubricant.consensus.raft.support.TimeLimited;
import io.lubricant.consensus.raft.transport.EventNode;
import io.lubricant.consensus.raft.transport.event.*;
import io.netty.channel.Channel;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * 异步服务（将异步请求转换为事件）
 */
public class AsyncService {

    protected final EventNode node;
    private final AtomicInteger sequence;
    private final ConcurrentMap<InvokeID, Invocation> invocations;

    protected AsyncService(EventNode node) {
        this.node = node;
        this.sequence = new AtomicInteger();
        this.invocations = new ConcurrentHashMap<>();
    }

    private static class InvokeID {
        private final String scope;
        private final int sequence;
        InvokeID(String scope, int sequence) {
            this.scope = scope;
            this.sequence = sequence;
        }
        @Override
        public int hashCode() {
            return scope.hashCode() * 31 + sequence;
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof InvokeID) {
                InvokeID id = (InvokeID) obj;
                return id.sequence == sequence
                        && id.scope.equals(scope);
            }
            return false;
        }

        @Override
        public String toString() {
            return scope + '[' + sequence + ']';
        }
    }


    public class Invocation<V> extends CompletableFuture<V> implements Callable<V>, TimeLimited, BiConsumer<PongEvent, Class<V>> {

        private final InvokeID id;

        Invocation(InvokeID id) {
            this.id = id;
        }

        @Override
        public void timeout() {
            invocations.remove(id);
        }

        @Override
        @SuppressWarnings("all")
        public void accept(PongEvent event, Class<V> clazz) {
            if (id.sequence == event.sequence() &&
                id.scope.equals(event.source().scope()) &&
                node.id().equals(event.source().nodeID())) {
                try {
                    complete((V)event.message());
                } catch (Exception ex) {
                    completeExceptionally(ex);
                }
            } else {
                throw new IllegalArgumentException(event.source() + " != " + id);
            }
        }

        @Override
        public V call() throws Exception {
            return get();
        }
    }

    @SuppressWarnings("all")
    protected <V> Invocation<V> remove(String scope, int sequence) {
        return invocations.remove(new InvokeID(scope, sequence));
    }

    protected <V> Async<V> invoke(String scope, Object params) {
        final EventID eventID = new EventID(scope, node.id());
        final InvokeID invokeID = new InvokeID(eventID.scope(), sequence.getAndIncrement());
        final Invocation<V> invocation = new Invocation<>(invokeID);
        return Async.call( ignored -> {
            Channel channel = node.channel();
            if (channel != null && channel.isWritable()) {
                invocations.put(invokeID, invocation);
                try {
                    channel.writeAndFlush(new PingEvent(eventID, params, invokeID.sequence));
                } catch (Exception e) {
                    Invocation i = invocations.remove(invokeID);
                    if (i != null) {
                        i.completeExceptionally(e);
                    }
                }
            } else {
                invocation.cancel(true);
            }
        }, invocation, decorateInvocation(invocation));
    }

    /**
     * 在 Invocation 中织入超时功能
     */
    private <T> Async.FutureFactory<T> decorateInvocation(Invocation<T> invocation) {
        return (call, callback, timer, head) -> {
            Async.AsyncFuture<T> future = new Async.AsyncFuture<T>(call, callback, timer, head) {
                @Override
                public void timeout() {
                    super.timeout(); // 共享 timeout 定时器，减少不必要定时任务
                    invocation.timeout();
                }
            };
            invocation.thenRun(future); // RPC 完成后通知调用者
            return future;
        };
    }

}