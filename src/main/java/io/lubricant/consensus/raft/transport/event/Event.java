package io.lubricant.consensus.raft.transport.event;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 基础事件
 */
public interface Event<Message> {

    Message message();

    /**
     * 结尾事件
     * */
    interface Ending {}

    /**
     * 带序号的二进制事件
     */
    abstract class SeqEvent extends BinEvent {

        private int sequence;

        public SeqEvent(EventID source, Object message, int sequence) {
            super(source, message);
            this.sequence = sequence;
        }

        public int sequence() {
            return sequence;
        }

        @Override
        public String toString() {
            return source().toString() + '[' +
                    Integer.toHexString(sequence) + ']';
        }
    }

    /**
     * 二进制事件
     */
    abstract class BinEvent implements Event<Object> {

        private EventID source;
        private Object message;

        public BinEvent(EventID source, Object message) {
            this.source = source;
            this.message = message;
        }

        public EventID source() {
            return source;
        }

        @Override
        public Object message() {
            return message;
        }
    }

    /**
     * URI事件
     */
    abstract class UrlEvent extends StrEvent {

        protected UrlEvent() { super(null); }

        protected String encode(String s) {
            try {
                return URLEncoder.encode(s == null ? "": s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new Error(e);
            }
        }

        protected String decode(String s) {
            try {
                return URLDecoder.decode(s == null ? "": s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new Error(e);
            }
        }

        protected void setParams(Map<String,String> params) {
            message = params.entrySet().stream()
                    .map(e -> encode(e.getKey()) + '=' + encode(e.getValue()))
                    .collect(Collectors.joining("&"));
        }

        protected Map<String,String> getParams(String message) {
            Map<String,String> params = Collections.emptyMap();
            if ((this.message = message) != null) {
                params = Arrays.stream(message.split("&"))
                        .map(p -> p.split("="))
                        .reduce(new HashMap<>(),
                                (map, kv)-> {map.put(decode(kv[0]), decode(kv[1])); return map;},
                                (m1, m2) -> {m1.putAll(m2); return m1;});
            }
            return params;
        }
    }

    /**
     * 字符串事件
     */
    abstract class StrEvent implements Event<String> {

        protected String message;

        public StrEvent(String message) {
            this.message = message;
        }

        @Override
        public String message() {
            return message;
        }
    }

}
