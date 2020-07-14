package io.lubricant.consensus.raft.support.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;
import io.netty.buffer.ByteBuf;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;


/**
 * 序列化
 */
public class Serialization {

    private static final KryoPool KRYO_POOL = new KryoPool.Builder(() -> {
        final Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(
                new StdInstantiatorStrategy()));
        return kryo;
    }).softReferences().build();

    private static final ThreadLocal<SpaceOutput> OUTPUT =
            ThreadLocal.withInitial(SpaceOutput::new);

    private static final ThreadLocal<SpaceInput> INPUT =
            ThreadLocal.withInitial(SpaceInput::new);

    /**
     * 序列化对象
     * @param message 消息对象
     */
    public static byte[] writeObject(Object message) throws SerializeException {
        return writeObject(null, message);
    }

    /**
     * 序列化对象
     * @param meta 写入头部的元数据
     * @param message 消息对象
     */
    public static byte[] writeObject(byte[] meta, Object message) throws SerializeException {
        Kryo kryo = KRYO_POOL.borrow();
        SpaceOutput output = OUTPUT.get();
        try {
            ByteArrayOutputStream bao = new ByteArrayOutputStream(64);
            if (meta != null) {
                bao.write(meta);
            }
            Output out = output.wrap(bao);
            kryo.writeClassAndObject(out, message);
            out.flush();
            return bao.toByteArray();
        } catch (Exception ex) {
            throw new SerializeException(ex);
        } finally {
            KRYO_POOL.release(kryo);
            output.clear();
        }
    }

    /**
     * 反序列化二进制数据
     * @param bytes 二进制数据
     */
    @SuppressWarnings("unchecked")
    public static <T> T readObject(byte[] bytes) throws SerializeException {
        return readObject(0, bytes);
    }

    /**
     * 反序列化二进制数据
     * @param skip 跳过字节数
     * @param bytes 二进制数据
     */
    @SuppressWarnings("unchecked")
    public static <T> T readObject(int skip, byte[] bytes) throws SerializeException {
        Kryo kryo = KRYO_POOL.borrow();
        try {
            return (T) kryo.readClassAndObject(new Input(bytes, skip, bytes.length - skip));
        } catch (Exception ex) {
            throw new SerializeException(ex);
        } finally {
            KRYO_POOL.release(kryo);
        }
    }

    /**
     * 序列化对象并写入 ByteBuf
     * @param msg 消息对象
     * @param buf 缓冲对象
     */
    public static void writeObject(Object msg, ByteBuf buf) throws SerializeException {
        Kryo kryo = KRYO_POOL.borrow();
        SpaceOutput output = OUTPUT.get();
        try {
            Output out = output.wrap(buf);
            kryo.writeClassAndObject(out, msg);
            out.flush();
        } catch (Exception ex) {
            throw new SerializeException(ex);
        } finally {
            KRYO_POOL.release(kryo);
            output.clear();
        }
    }

    /**
     * 反序列化 ByteBuf 中的数据
     * @param buf 缓冲对象
     * @param len 数据长度
     */
    @SuppressWarnings("unchecked")
    public static <T> T readObject(ByteBuf buf, int len) throws SerializeException {
        Kryo kryo = KRYO_POOL.borrow();
        SpaceInput input = INPUT.get();
        try {
            Input in = input.wrap(buf, len);
            return (T) kryo.readClassAndObject(in);
        } catch (Exception ex) {
            throw new SerializeException(ex);
        } finally {
            KRYO_POOL.release(kryo);
            input.clear();
        }
    }

}