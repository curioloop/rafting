package io.lubricant.consensus.raft.support.serial;

import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;

import java.io.InputStream;

public class SpaceInput extends InputStream {

    private Input in = new Input(2048);
    private ByteBuf space;
    private int length;
    private int position;

    public Input wrap(ByteBuf space, int len) {
        in.setInputStream(this);
        this.space = space;
        this.length = len;
        this.position = 0;
        return in;
    }

    public void clear() {
        this.space = null;
        this.length = 0;
    }

    @Override
    public int read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buf, int off, int len) {
        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int pos = position;
        int end = Math.min(pos + len, length);
        if (end == pos) {
            return -1;
        }
        space.readBytes(buf, off, end - pos);
        return (position = end) - pos;
    }
}
