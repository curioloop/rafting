package io.lubricant.consensus.raft.transport.event;

public class OneWayEvent extends Event.BinEvent {

    public OneWayEvent(EventID source, Object message) {
        super(source, message);
    }
}
