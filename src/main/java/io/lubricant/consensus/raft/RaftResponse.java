package io.lubricant.consensus.raft;

import java.io.Serializable;

/**
 * 响应数据
 */
public class RaftResponse implements Serializable {

    private final long term;
    private final boolean success;

    private RaftResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public long term() { return term; }
    public boolean success() { return success; }
    public String toString() { return "RaftResponse(" + term + ":" + success + ")"; }

    public static RaftResponse success(long term) { return new RaftResponse(term, true); }
    public static RaftResponse failure(long term) { return new RaftResponse(term, false); }
    public static RaftResponse reply(long term, boolean result) { return new RaftResponse(term, result); }
}