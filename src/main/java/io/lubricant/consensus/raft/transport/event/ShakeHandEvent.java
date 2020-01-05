package io.lubricant.consensus.raft.transport.event;

public class ShakeHandEvent extends Event.StrEvent {

    public ShakeHandEvent(String message) {
        super(message);
    }

    public boolean isOK() {
        return "OK".equals(message());
    }

    public static ShakeHandEvent ok() {
        return new ShakeHandEvent("OK");
    }
}
