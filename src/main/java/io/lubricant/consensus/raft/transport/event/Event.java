package io.lubricant.consensus.raft.transport.event;

/**
 * 基础事件
 */
public interface Event<Message> {

    Message message();

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
     * 字符串事件
     */
    abstract class StrEvent implements Event<String> {

        private String message;

        public StrEvent(String message) {
            this.message = message;
        }

        @Override
        public String message() {
            return message;
        }
    }

}
