package io.lubricant.consensus.raft.transport.event;

public class PingEvent extends Event.SeqEvent {

    public PingEvent(EventID source, Object message, int sequence) {
        super(source, message, sequence);
    }
}
