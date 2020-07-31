package io.lubricant.consensus.raft.support.serial;

import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class SpaceOutput extends OutputStream {

    private Output out = new Output(2048);
    private ByteBuf space;
    private DataOutput data;

    public Output wrap(DataOutput data) {
        out.setOutputStream(this);
        this.data = data;
        return out;
    }

    public Output wrap(ByteBuf space) {
        out.setOutputStream(this);
        this.space = space;
        return out;
    }

    public Output wrap(OutputStream os) {
        out.setOutputStream(os);
        return out;
    }

    public void clear() {
        out.setOutputStream(null);
        this.space = null;
        this.data = null;
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] buf, int off, int len)  {
        if (buf == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > buf.length) || (len < 0) ||
                ((off + len) > buf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        if (space != null) {
            space.writeBytes(buf, off, len);
        } else try {
            data.write(buf, off, len);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
