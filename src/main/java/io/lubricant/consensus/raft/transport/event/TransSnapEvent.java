package io.lubricant.consensus.raft.transport.event;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class TransSnapEvent extends Event.UrlEvent implements Event.Ending {

    private final String reason;
    private final long index, term, length;
    private final RandomAccessFile raf; // FileRegion don`t close RAF, so we need to close it manually

    public TransSnapEvent(long index, long term, Path path) throws IOException {
        this.raf = new RandomAccessFile(path.toFile(), "r");
        this.reason = null;
        Map<String, String> params = new HashMap<>();
        params.put("index", Long.toString(this.index = index));
        params.put("term", Long.toString(this.term = term));
        params.put("length", Long.toString(this.length = raf.length()));
        setParams(params);
    }

    public TransSnapEvent(long index, long term, String reason) {
        this.raf = null;
        Map<String, String> params = new HashMap<>();
        params.put("index", Long.toString(this.index = index));
        params.put("term", Long.toString(this.term = term));
        params.put("length", Long.toString(this.length = -1L));
        params.put("reason", this.reason = reason);
        setParams(params);
    }

    public TransSnapEvent(String message) {
        Map<String, String> params = getParams(message);
        this.index = Long.parseLong(params.get("index"));
        this.term = Long.parseLong(params.get("term"));
        this.length = Long.parseLong(params.get("length"));
        this.reason = params.get("reason");
        this.raf = null;
    }

    public long index() {
        return index;
    }

    public long term() {
        return term;
    }

    public long length() {
        return length;
    }

    public String reason() {
        return reason;
    }

    public RandomAccessFile file() { return raf; }

    public boolean isOK() { return length != -1L; }

}
