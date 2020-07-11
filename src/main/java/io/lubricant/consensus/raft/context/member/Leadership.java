package io.lubricant.consensus.raft.context.member;

import io.lubricant.consensus.raft.RaftParticipant;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.transport.RaftCluster.ID;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public interface Leadership {

    int REPLICATE_LIMIT = 10; // 一次传输的日志数量

    AtomicLongFieldUpdater<State>
            lastRequest = AtomicLongFieldUpdater.newUpdater(State.class, "lastRequest"),
            requestSuccess = AtomicLongFieldUpdater.newUpdater(State.class, "requestSuccess"),
            requestFailure = AtomicLongFieldUpdater.newUpdater(State.class, "requestFailure");

    AtomicIntegerFieldUpdater<State>
            requestInFlight = AtomicIntegerFieldUpdater.newUpdater(State.class, "requestInFlight"),
            recentFailure = AtomicIntegerFieldUpdater.newUpdater(State.class, "recentFailure");

    /**
     * 其他节点的状态
     */
    class State {

        volatile long lastRequest; // 最近一次发送心跳的时间
        volatile long requestSuccess; // 最近一次发送心跳成功的时间
        volatile long requestFailure; // 最近一次发送心跳失败的时间
        volatile int requestInFlight; // 在途请求数量
        volatile int recentFailure;   // 最近连续请求失败次数

        volatile long lastEpoch; // 已知的最新的日志起点
        volatile long nextIndex; // 下一条要发送的日志（初始为最后一条日志的 index）
        volatile long matchIndex; // 已经复制成功的最后一条日志（初始为 0）
        volatile boolean pendingInstallation; // 等待数据同步完成

        boolean increaseMono(AtomicLongFieldUpdater<State> field, long current, long next) {
            return (next > current) && field.compareAndSet(this, current, next);
        }

        boolean isUnhealthy(int criticalPoint, long coolDown, long now) {
            return criticalPoint > 0 && Integer.compareUnsigned(recentFailure, criticalPoint) > 0 ||
                   coolDown > 0 && now - requestFailure < coolDown;
        }

        boolean isReady(int criticalPoint, long coolDown, long now) {
            return requestSuccess != 0 && ! (pendingInstallation || isUnhealthy(criticalPoint, coolDown, now));
        }

        void statSuccess(long now) {
            increaseMono(Leadership.requestSuccess, requestSuccess, now);
            if (recentFailure != 0) {
                recentFailure = 0;
            }
        }

        void statFailure(long now) {
            increaseMono(Leadership.requestFailure, requestFailure, now);
            Leadership.recentFailure.incrementAndGet(this);
        }

        synchronized void updateIndex(long epoch, long index, boolean success, boolean snapshot) {
            if (index < matchIndex) {
                throw new AbstractMethodError(String.format(
                        "match index should not rollback: [%d %d %d %b] (%d %d %b %b)",
                        lastEpoch, nextIndex, matchIndex, pendingInstallation,
                        epoch, index, success, snapshot));
            }

            if (epoch < lastEpoch) return;
            if (epoch > lastEpoch) {
                lastEpoch = epoch;
                nextIndex = Math.max(nextIndex, epoch + 1);
            }

            if (pendingInstallation != snapshot) return;

            if (pendingInstallation) {
                if (success) {
                    nextIndex = Math.max(nextIndex, epoch + 1);
                    pendingInstallation = false;
                }
            } else {
                if (success) {
                    if (index > matchIndex) {
                        nextIndex = index + 1;
                        matchIndex = index;
                    }
                } else if (matchIndex == 0) {
                    long next = Math.max(nextIndex - REPLICATE_LIMIT, epoch + 1);
                    nextIndex = Math.min(nextIndex - 1, next);
                    if (nextIndex <= epoch && ! pendingInstallation) {
                        pendingInstallation = true;
                    }
                }
            }
        }

        static long[] majorIndices(Collection<State> states) {
            long[] matchIndices = states.stream().mapToLong(st -> st.matchIndex).toArray();
            Arrays.sort(matchIndices); // 将复制成功的最后一条日志索引，按照从小到大进行排序
            // 第 majority 小的元素，表示至少有 majority 个节点的日志复制的索引均已经帮会这条日志
            // N = states.size() + 1 表示节点总数，o 表示 majority 对应的索引，* 表示 leader 对应的索引
            // N = 2, major = 2 : |o|*|
            // N = 3, major = 2 : |x|o|*|
            // N = 4, major = 3 : |x|o|x|*|
            // N = 5, major = 3 : |x|x|o|x|*|
            // N = 6, major = 4 : |x|x|o|x|x|*|
            // N = 7, major = 4 : |x|x|x|o|x|x|*|
            int majorIndex = matchIndices.length / 2;
            // 返回两个索引：[在所有节点都复制成功的日志索引，在大多数节点复制成功的日志索引]
            return new long[] {matchIndices[0], matchIndices[majorIndex]};
        }
    }

    /**
     * 未就绪异常（当前集群的健康节点数不足时抛出）
     */
    class NotReadyException extends Exception {

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }

    }

    /**
     * 非主异常（当前节点上的角色不是 Leader 时抛出）
     */
    class NotLeaderException extends Exception {

        private final ID currentLeader;

        public NotLeaderException(RaftParticipant participant) {
            if (participant instanceof Follower) {
                currentLeader = ((Follower)participant).currentLeader();
            } else {
                currentLeader = null;
            }
        }

        public ID currentLeader() {
            return currentLeader;
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }

    }

}
