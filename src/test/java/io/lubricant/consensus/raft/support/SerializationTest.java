package io.lubricant.consensus.raft.support;

import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.support.serial.Serialization;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SerializationTest {

    static class TestEntry implements RaftLog.Entry {
        private final List<String> list = new ArrayList<>();
        private long index, term;

        TestEntry() {
            this.index = 123;
            this.term = 456;
            list.add("Hello");
        }

        TestEntry(int n) {
            this.index = n;
            this.term = n;
            list.add("World");
        }

        @Override
        public long index() {
            return index;
        }
        @Override
        public long term() {
            return term;
        }

    }

    @Test
    public void testEntry() throws SerializeException {
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        RaftLog.Entry[] entries = {new TestEntry(789)};
        Object p = new Object[]{1L, "ABC", entries};
        buf.clear();
        Serialization.writeObject(p, buf);
        Object q = Serialization.readObject(buf, buf.writerIndex());

        Object[] n = (Object[]) q;
        System.out.println(n[0]);
        System.out.println(n[1]);
        RaftLog.Entry[] entry = (RaftLog.Entry[]) n[2];
        System.out.println(entry[0].index());
        System.out.println(entry[0].term());
    }

    @Test
    public void testByteBuf() throws SerializeException {
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        Serialization.writeObject("123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456123456", buf);
        System.out.println(buf.writerIndex());
        Object o = Serialization.readObject(buf, buf.writerIndex());
        System.out.println(o);

        buf = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        Serialization.writeObject("789789789789789789789789789789789789789789789", buf);
        System.out.println(buf.writerIndex());
        o = Serialization.readObject(buf, buf.writerIndex());
        System.out.println(o);
    }

    @Test
    public void testSkip() throws SerializeException {
        String hello = "hello";
        String world = "world";
        byte[] meta = hello.getBytes();
        byte[] data = Serialization.writeObject(meta, world);
        Object o = Serialization.readObject(meta.length, data);
        System.out.println(o);
    }

}
