package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.support.serial.Serialization;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;
import io.lubricant.consensus.raft.transport.event.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 事件编码器
 */
public class EventCodec {

    private final static Logger logger = LoggerFactory.getLogger(EventCodec.class);

    public static final String STR_EVENT_DECODER = "strEventDecoder";
    public static final String BIN_EVENT_DECODER = "binEventDecoder";

    private static final int MAX_HEAD_SIZE = 128;
    private static final int MAX_BODY_SIZE = 1 << 26; // 64MB

    private final static byte NUL = 0x00; // Null
    // frame
    private final static byte SOH = 0x01; // Start Of Headline
    private final static byte STX = 0x02; // Start Of Text
    private final static byte ETX = 0x03; // End Of Text
    private final static byte EOT = 0x04; // End Of Transmission
    // event
    private final static byte ENQ = 0x05; // Enquiry
    private final static byte ACK = 0x06; // Acknowledge
    private final static byte SYN = 0x16; // Synchronous Idle
    // snap
    private final static byte MW = (byte)0x95; // Message Waiting
    private final static byte PM = (byte)0x9E; // Private Message

    private static final class EventFrame {
        byte type = NUL;
        boolean isEnding;
        boolean hasSequence;
        int sequence;
        int headLength = -1;
        int bodyLength = -1;
        String head;
        Object body;
    }

    public static MessageToMessageEncoder<Event> eventEncoder() {
        return new EventEncoder();
    }

    public static MessageToMessageDecoder<EventFrame> strEventDecoder() {
        return new StrEventDecoder();
    }

    public static MessageToMessageDecoder<EventFrame> binEventDecoder(NodeID nodeID) {
        return new BinEventDecoder(nodeID);
    }

    static class EventEncoder extends MessageToMessageEncoder<Event> {
        @Override
        protected void encode(ChannelHandlerContext context, Event event, List<Object> list) throws Exception {
            EventFrame frame = null;
            if (event instanceof Event.BinEvent) {
                Event.BinEvent e = (Event.BinEvent) event;
                frame = new EventFrame();
                frame.head = e.source().scope();
                frame.body = e.message();

                if (event instanceof PingEvent) {
                    frame.type = ENQ;
                } else if (event instanceof PongEvent) {
                    frame.type = ACK;
                } else {
                    logger.error("unknown BinEvent : {}", event);
                    return;
                }

            } else if (event instanceof Event.StrEvent) {
                Event.StrEvent e = (Event.StrEvent) event;
                frame = new EventFrame();
                frame.head = e.message();
                frame.bodyLength = 0;

                if (event instanceof ShakeHandEvent) {
                    frame.type = SYN;
                } else if (event instanceof WaitSnapEvent) {
                    frame.type = MW;
                } else if (event instanceof TransSnapEvent) {
                    frame.type = PM;
                } else {
                    logger.error("unknown StrEvent : {}", event);
                    return;
                }
            }

            if (frame != null) {
                frame.isEnding = event instanceof Event.Ending;
                frame.headLength = frame.head.length();
                if (event instanceof Event.SeqEvent) {
                    frame.hasSequence = true;
                    frame.sequence = ((Event.SeqEvent) event).sequence();
                }
                list.add(frame);
            } else {
                logger.error("unknown Event : {}", event);
            }
        }
    }

    static class StrEventDecoder extends MessageToMessageDecoder<EventFrame> {

        @Override
        protected void decode(ChannelHandlerContext context, EventFrame frame, List<Object> list) throws Exception {
            if (frame.type == SYN) {
                list.add(new ShakeHandEvent(frame.head));
            } else if (frame.type == MW) {
                list.add(new WaitSnapEvent(frame.head));
            } else if (frame.type == PM) {
                list.add(new TransSnapEvent(frame.head));
            } else {
                logger.error("unsupported frame type " + frame.type);
            }
        }
    }

    static class BinEventDecoder extends StrEventDecoder {

        final NodeID nodeID;

        BinEventDecoder(NodeID nodeID) {
            this.nodeID = nodeID;
        }

        @Override
        protected void decode(ChannelHandlerContext context, EventFrame frame, List<Object> list) throws Exception {
            if (frame.type == SYN) {
                super.decode(context, frame, list);
            } else {
                Event event;
                EventID id = new EventID(frame.head, nodeID);
                switch (frame.type) {
                    case ENQ: event = new PingEvent(id, frame.body, frame.sequence);
                        break;
                    case ACK: event = new PongEvent(id, frame.body, frame.sequence);
                        break;
                    default:
                        logger.error("unsupported frame type " + frame.type);
                        return;
                }
                list.add(event);
            }
        }
    }

    public static MessageToByteEncoder<EventFrame> frameEncoder() {
        return new FrameEncoder();
    }

    public static ByteToMessageDecoder frameDecoder() {
        return new FrameDecoder();
    }

    static class FrameEncoder extends MessageToByteEncoder<EventFrame> {
        @Override
        protected void encode(ChannelHandlerContext context, EventFrame event, ByteBuf buf) throws Exception {
            buf.markWriterIndex();
            try {
                buf.writeByte(SOH);
                buf.writeByte(event.type);
                if (event.hasSequence) {
                    buf.writeInt(event.sequence);
                }
                buf.writeByte(STX);
                // write head
                int i = buf.writerIndex();
                buf.writeInt(0);
                int len = buf.writeCharSequence(event.head, StandardCharsets.UTF_8);
                buf.setInt(i, len);
                // write body
                i = buf.writerIndex();
                buf.writeInt(0);
                if (event.body != null) {
                    Serialization.writeObject(event.body, buf);
                    len = buf.writerIndex() - i - Integer.BYTES;
                    buf.setInt(i, len);
                }
                buf.writeByte(ETX);
                if (event.isEnding) {
                    buf.writeByte(EOT);
                }
            } catch (Exception e) {
                buf.resetWriterIndex();
                logger.error("encode event frame error", e);
            }
        }
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext context, EventFrame event, boolean preferDirect) throws Exception {
            int initialCapacity;
            if (event.bodyLength >= 0) {
                initialCapacity = 12 + event.headLength + event.bodyLength;
            } else {
                initialCapacity = 1024; // unknown body len
            }
            if (preferDirect) {
                return context.alloc().ioBuffer(initialCapacity);
            } else {
                return context.alloc().heapBuffer(initialCapacity);
            }
        }

    }

    static class FrameDecoder extends ByteToMessageDecoder {

        private boolean transparent = false;
        private EventFrame state;

        private void verify(ByteBuf buf, byte expect) {
            byte result = buf.readByte();
            if (result != expect) {
                throw new DecoderException(result + " != " + expect);
            }
        }

        private byte verify(ByteBuf buf, byte expect, byte option) {
            byte result = buf.readByte();
            if (result != expect && result != option) {
                throw new DecoderException(result + " != " + expect + "," + option);
            }
            return result;
        }

        private void readType(ByteBuf buf, EventFrame state) {
            switch (state.type = buf.readByte()) {
                case ENQ: case ACK:
                    state.hasSequence = true;
            }
        }

        private void readSequence(ByteBuf buf, EventFrame state) {
            state.sequence = buf.readInt();
            state.hasSequence = false;
        }

        private void readHeadLen(ByteBuf buf, EventFrame state) {
            verify(buf, STX);
            int length = buf.readInt();
            if (length < 0 || length > MAX_HEAD_SIZE) {
                throw new DecoderException("illegal head length " + length);
            }
            state.headLength = length;
        }

        private void readBodyLen(ByteBuf buf, EventFrame state) {
            CharSequence head = buf.readCharSequence(state.headLength, StandardCharsets.UTF_8);
            int length = buf.readInt();
            if (length < 0 || length > MAX_BODY_SIZE) {
                throw new DecoderException("illegal body length " + length);
            }
            state.head = head.toString();
            state.bodyLength = length;
        }

        private void readBody(ByteBuf buf, EventFrame state) {
            if (state.bodyLength > 0) {
                try {
                    state.body = Serialization.readObject(buf, state.bodyLength);
                } catch (SerializeException e) {
                    throw new DecoderException("fail to deserialize body", e);
                }
            }
            verify(buf, ETX);
        }

        @Override
        protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> frames) throws Exception {
            if (transparent) {
                // 透传数据而不直接调用 ctx.pipeline().remove(FrameDecoder.class) 的原因：
                // 1. ctx.pipeline().remove 会触发 ctx.handler().handlerRemoved 回调，此时如果 buf 存在未读消息，会再次调用 decode 解析剩余数据
                // 2. 后续数据到达时，NioEventLoop.processSelectedKey 会异步触发 ByteToMessageDecoder.channelRead 间接调用 decode
                // 以上两种情况都会致使 decode 读到非法的字节流，导致解析失败
                context.fireChannelRead(buf);
                return;
            }
            // |SOH|TYPE|{SEQUENCE}|STX|HEAD_LEN|HEAD|BODY_LEN|{BODY}|ETX
            try {
                while (buf.isReadable()) {
                    if (state == null) {
                        if (transparent = (EOT == verify(buf, SOH, EOT))) {
                            return;
                        }
                        state = new EventFrame();
                    } else {
                        EventFrame s = state;
                        if (s.type == NUL) {
                            if (buf.readableBytes() < 1)
                                break;
                            readType(buf, s);
                        }
                        if (s.hasSequence) {
                            if (buf.readableBytes() < 4)
                                break;
                            readSequence(buf, s);
                        }
                        if (s.headLength == -1) {
                            if (buf.readableBytes() < 5)
                                break;
                            readHeadLen(buf, s);
                        }
                        if (s.bodyLength == -1) {
                            if (buf.readableBytes() < s.headLength + 4)
                                break;
                            readBodyLen(buf, s);
                        } else {
                            if (buf.readableBytes() < s.bodyLength + 1)
                                break;
                            readBody(buf, s);
                            frames.add(s);
                            state = null;
                        }
                    }
                }
            } catch (Exception e) {
                state = null;
                logger.error("decode event frame error", e);
                context.close(); // close channel
            }
        }
    }

}
