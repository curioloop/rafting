package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.context.member.Leader;
import io.lubricant.consensus.raft.context.member.Leadership.NotLeaderException;
import io.lubricant.consensus.raft.support.Promise;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 客户端（用户交互界面）
 */
public class RaftClient implements AutoCloseable {

    /**
     * 用户指令
     */
    public interface Command<R> extends Serializable {}

    private RaftContext context;
    private Map<String, RaftClient> parent;
    private volatile long refCount = 1;

    /**
     * 新建客户端
     * @param context 上下文
     * @param parent 客户端池
     */
    public RaftClient(RaftContext context, Map<String, RaftClient> parent) {
        this.context = context;
        this.parent = parent;
    }

    /**
     * 执行并等待返回
     * @param cmd 用户命令
     * @param timeout 超时时间
     * @return 执行结果
     * @throws TimeoutException 超时未响应
     * @throws ExecutionException 执行异常（具体原因可以用 getCause 获取）
     */
    public <R> R execute(Command<R> cmd, long timeout) throws ExecutionException, TimeoutException {
        try {
            return submit(cmd).get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecutionException("fail to execute command " +
                    cmd.getClass().getSimpleName(), e);
        }
    }

    /**
     * 提交命令
     * @param cmd 用户命令
     * @return 异步调用结果
     */
    public <R> Future<R> submit(Command<R> cmd) {
        Promise<R> promise = new Promise<>();
        FutureTask<R> future = new FutureTask<>(promise);
        promise.future(future);
        context.eventLoop().execute(() -> process(cmd, promise, context));
        return future;
    }

    /**
     * 处理命令
     */
    private void process(Command cmd, Promise promise, RaftContext ctx) {
        RaftParticipant participant = ctx.participant();
        if (participant instanceof Leader) {
            ((Leader) participant).acceptCommand(cmd, promise);
        } else {
            promise.error(new NotLeaderException(participant));
        }
    }

    public synchronized boolean refer() {
        if (refCount == 0)
            return false;
        refCount += 1;
        return true;
    }

    @Override
    public synchronized void close() throws Exception {
        if (--refCount == 0)
            parent.remove(context.ctxID(), this);
    }

}
