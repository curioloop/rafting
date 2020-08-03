package io.lubricant.consensus.raft.support;

public class RaftException extends RuntimeException {

    public RaftException() {super();}

    public RaftException(String msg) {super(msg);}

    public RaftException(Throwable ex) {super(ex);}

    public RaftException(String msg, Throwable ex) {super(msg, ex);}

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    @Override
    public String getMessage() {
        String msg = super.getMessage();
        if (msg == null) {
            msg = getClass().getSimpleName();
            int i = msg.lastIndexOf("Exception");
            if (i > 0) {
                msg = msg.substring(0, i);
            }
        }
        return msg;
    }
}