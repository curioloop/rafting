package io.lubricant.consensus.raft.support.serial;

import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

public class SpaceInput extends InputStream {

    private Input in = new Input(2048);
    private ByteBuf space;
    private DataInput data;
    private long length;
    private long position;

    public Input wrap(DataInput data, long len) {
        in.setInputStream(this);
        this.data = data;
        this.length = len;
        this.position = 0;
        return in;
    }

    public Input wrap(ByteBuf space, int len) {
        in.setInputStream(this);
        this.space = space;
        this.length = len;
        this.position = 0;
        return in;
    }

    public void clear() {
        this.data = null;
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

        long pos = position;
        long end = Math.min(pos + len, length);
        if (end == pos) {
            return -1;
        }

        if (space != null) {
            space.readBytes(buf, off, (int)(end - pos));
        } else try {
            data.readFully(buf, off, (int)(end - pos));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return (int) ((position = end) - pos);
    }
}
