package io.lubricant.consensus.raft.transport.event;

public class PongEvent extends Event.SeqEvent {

    public PongEvent(EventID source, Object message, int sequence) {
        super(source, message, sequence);
    }

}
