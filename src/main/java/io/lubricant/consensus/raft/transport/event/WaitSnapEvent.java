package io.lubricant.consensus.raft.transport.event;

import java.util.HashMap;
import java.util.Map;

public class WaitSnapEvent extends Event.UrlEvent {

    private final String context;
    private final long index, term;

    public WaitSnapEvent(String context, long index, long term) {
        Map<String, String> params = new HashMap<>();
        params.put("context", this.context = context);
        params.put("index", Long.toString(this.index = index));
        params.put("term", Long.toString(this.term = term));
        setParams(params);
    }

    public WaitSnapEvent(String message) {
        Map<String, String> params = getParams(message);
        this.context = params.get("context");
        this.index = Long.parseLong(params.get("index"));
        this.term = Long.parseLong(params.get("term"));
    }

    public String context() {
        return context;
    }

    public long index() {
        return index;
    }

    public long term() {
        return term;
    }

}
