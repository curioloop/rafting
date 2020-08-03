package io.lubricant.consensus.raft.command;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.context.member.Leader;
import io.lubricant.consensus.raft.support.Promise;
import io.lubricant.consensus.raft.support.RaftException;
import io.lubricant.consensus.raft.support.anomaly.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * 服务存根（用户交互界面）
 */
public class RaftStub implements AutoCloseable {

    /**
     * 用户指令
     */
    public interface Command<R> extends Serializable {}

    private RaftContext context;
    private Map<String, RaftStub> parent;
    private volatile long refCount = 1;

    /**
     * 新建客户端
     * @param context 上下文
     * @param parent 客户端池（为 null 时忽略引用计数）
     */
    public RaftStub(RaftContext context, Map<String, RaftStub> parent) {
        this.context = Objects.requireNonNull(context);
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
        } catch (TimeoutException | ExecutionException | RaftException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof RaftException)
                throw (RaftException) e.getCause();
            throw new ExecutionException("fail to execute command " +
                    cmd.getClass().getSimpleName(), e);
        }
    }

    /**
     * 提交命令
     * @param cmd 用户命令
     * @return 异步调用结果
     */
    public <R> Promise<R> submit(Command<R> cmd) {
        Promise<R> promise = new Promise<>(context.envConfig().broadcastTimeout());
        if (context.stillRunning() && context.eventLoop().isAvailable()) {
            context.eventLoop().execute(() -> process(cmd, promise, context));
        } else  {
            promise.completeExceptionally( ! context.stillRunning() ? new ObsoleteContextException():
                    context.eventLoop().isBusy() ? new BusyLoopException(): new ExistedLoopException());
        }
        return promise;
    }

    /**
     * 处理命令
     */
    private void process(Command cmd, Promise promise, RaftContext ctx) {
        RaftParticipant participant = ctx.participant();
        if (participant instanceof Leader) {
            Leader leader = (Leader) participant;
            if (leader.isReady()) {
                leader.acceptCommand(cmd, promise);
            } else {
                promise.completeExceptionally(new NotReadyException());
            }
        } else {
            promise.completeExceptionally(new NotLeaderException(participant));
        }
    }

    public synchronized boolean refer() {
        if (parent == null)
            return true;
        if (refCount == 0)
            return false;
        refCount += 1;
        return true;
    }

    @Override
    public synchronized void close() throws Exception {
        if (parent == null)
            return;
        if (--refCount == 0)
            parent.remove(context.ctxID(), this);
    }

}
