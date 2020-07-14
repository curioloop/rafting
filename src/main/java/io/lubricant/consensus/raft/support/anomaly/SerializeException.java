package io.lubricant.consensus.raft.support.anomaly;


/**
 * 序列化异常
 */
public class SerializeException extends Exception {

    public SerializeException(String msg) {
        super(msg);
    }

    public SerializeException(Exception ex) {
        super(ex);
    }

}
