package io.lubricant.consensus.raft.command.admin.domain;


public enum  CtxStatus {

    NOT_FOUND,
    NORMAL,
    SLEEPING,
    DESTROYED;

    public static CtxStatus from(String status) {
        if (status == null) return NOT_FOUND;
        return valueOf(status.toUpperCase());
    }

    public static String to(CtxStatus status) {
        if (status == NOT_FOUND) return null;
        return status.name();
    }
}
