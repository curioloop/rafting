package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.support.PendingTask;
import io.lubricant.consensus.raft.support.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.transport.event.NodeID;
import io.lubricant.consensus.raft.transport.event.ShakeHandEvent;
import io.lubricant.consensus.raft.transport.event.TransSnapEvent;
import io.lubricant.consensus.raft.transport.event.WaitSnapEvent;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * 事件节点（向其他节点发送事件）
 */
public class EventNode {

    class EventChannel {

        private final Logger logger = LoggerFactory.getLogger(EventChannel.class);

        volatile boolean ready;
        volatile Channel channel;

        public void connect() {
            new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.config()
                                    .setTcpNoDelay(true)
                                    .setWriteBufferWaterMark(new WriteBufferWaterMark(2 * 1024 * 1024, 5 * 1024 * 1024));
                            ch.pipeline()
                                    .addLast(EventCodec.frameDecoder())
                                    .addLast(EventCodec.strEventDecoder())
                                    .addLast(EventCodec.frameEncoder())
                                    .addLast(EventCodec.eventEncoder())
                                    .addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            logger.info("Shake hand with {}", dst);
                                            ctx.writeAndFlush(new ShakeHandEvent(src.toString()));
                                            super.channelActive(ctx);
                                        }
                                        @Override
                                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                            if (msg instanceof ShakeHandEvent) {
                                                ShakeHandEvent shakeHand = ((ShakeHandEvent) msg);
                                                if (shakeHand.isOK()) {
                                                    ready = true;
                                                    logger.info("Shake hand with {} completed", dst);
                                                } else {
                                                    logger.error("Shake hand with {} refused: {}", dst, shakeHand.message());
                                                    ctx.close();
                                                }
                                            }
                                        }
                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                            ready = false;
                                            logger.error("Error occurred at {}", dst, cause);
                                            ctx.close();
                                        }
                                        @Override
                                        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                            ready = false;
                                            logger.error("Disconnected from {}", dst);
                                            super.channelInactive(ctx);
                                        }
                                        @Override
                                        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                                            if (shutdown) {
                                                logger.error("Shutdown channel {}", dst);
                                                return;
                                            }
                                            // logger.error("Reconnect to {}", dst);
                                            Runnable reconnect = EventNode.this::reconnect;
                                            ctx.executor().schedule(reconnect, 1000, TimeUnit.MILLISECONDS);
                                            ctx.channel().pipeline().remove(this);
                                            super.channelUnregistered(ctx);
                                        }
                                    });
                        }
                    })
                    .connect(new InetSocketAddress(dst.hostname(), dst.port()))
                    .addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                channel = future.channel();
                                logger.info("Established event channel with {}", dst);
                            }
                        }
                    });
        }

        public void close() {
            shutdown = true;
            if (channel != null) {
                channel.close();
            }
        }

    }

    class SnapChannel extends PendingTask<Snapshot> {

        private final Logger logger = LoggerFactory.getLogger(SnapChannel.class);

        private WaitSnapEvent event;

        private long index, term;
        private Path path;

        private long position;
        private long count;

        private Channel channel;
        private RandomAccessFile raf;
        private FileChannel file;

        public SnapChannel(WaitSnapEvent event) {
            this.event = event;
        }

        @Override
        protected void perform() {
            new Bootstrap().group(snapGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.config()
                                    .setWriteBufferWaterMark(new WriteBufferWaterMark(5 * 1024 * 1024, 5 * 1024 * 1024));
                            ch.pipeline()
                                    .addLast(EventCodec.frameDecoder())
                                    .addLast(EventCodec.strEventDecoder())
                                    .addLast(EventCodec.frameEncoder())
                                    .addLast(EventCodec.eventEncoder())
                                    .addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            logger.info("Wait snap from {}", dst);
                                            ctx.writeAndFlush(new WaitSnapEvent(event.context(), event.index(), event.term()));
                                            super.channelActive(ctx);
                                        }
                                        @Override
                                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                            if (msg instanceof TransSnapEvent) {
                                                TransSnapEvent transSnap = ((TransSnapEvent) msg);
                                                if (transSnap.isOK()) {
                                                    logger.info("Trans snap from {}", dst);
                                                    term = transSnap.term();
                                                    index = transSnap.index();
                                                    count = transSnap.length();
                                                    path = Files.createTempFile(event.context(), ".snapshot");
                                                    if (count == 0) {
                                                        set(new Snapshot(path, index, term));
                                                        ctx.close();
                                                    } else {
                                                        raf = new RandomAccessFile(path.toFile(), "rw");
                                                        SnapChannel.this.file = raf.getChannel();
                                                    }
                                                } else {
                                                    logger.error("Trans snap from {} failed: {}", dst, transSnap.reason());
                                                    ctx.close();
                                                }
                                            } else if (file != null) {
                                                if (count - position >= 0 && position >= 0) {
                                                    ByteBuf buf = (ByteBuf) msg;
                                                    position += buf.readBytes(file, position, buf.readableBytes());
                                                    if (count - position == 0) {
                                                        close();
                                                        set(new Snapshot(path, index, term));
                                                        ctx.close();
                                                    }
                                                } else {
                                                    throw new IllegalArgumentException("position out of range: " + position + " (expected: 0 - " + (count - 1L) + ')');
                                                }
                                            }
                                        }
                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                            logger.error("Error occurred at {}", dst, cause);
                                            setException(cause);
                                            ctx.close();
                                        }
                                        @Override
                                        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                            logger.error("Disconnected from {}", dst);
                                            super.channelInactive(ctx);
                                        }


                                    });
                        }
                    })
                    .connect(new InetSocketAddress(dst.hostname(), dst.port()))
                    .addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                channel = future.channel();
                                logger.info("Established snap channel with {}", dst);
                            }
                        }
                    });
        }

        synchronized boolean isOpen() {
            return file != null && file.isOpen();
        }

        synchronized void close() throws IOException {
            if (file != null) {
                raf.close();
                file = null;
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (super.cancel(mayInterruptIfRunning)) {
                channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (isOpen()) {
                            close();
                        }
                    }
                });
                return true;
            }
            return false;
        }

    }

    private final NodeID src, dst;
    private final NioEventLoopGroup group;
    private final NioEventLoopGroup snapGroup;
    private volatile EventChannel eventChannel;
    private volatile boolean shutdown;
    private volatile boolean initialized;

    public EventNode(NodeID src, NodeID dst, NioEventLoopGroup group, NioEventLoopGroup snapGroup) {
        this.src = src;
        this.dst = dst;
        this.group = group;
        this.snapGroup = snapGroup;
    }

    public synchronized void connect() {
        if (! initialized) {
            reconnect();
            initialized = true;
        }
    }

    private void reconnect() {
        if (! shutdown) {
            eventChannel = new EventChannel();
            eventChannel.connect();
        }
    }

    public NodeID id() {
        return dst;
    }

    public Channel channel() {
        EventChannel ec = eventChannel;
        return ec != null && ec.ready ? ec.channel: null;
    }

    public void close() {
        shutdown = true;
        if (eventChannel != null) {
            eventChannel.close();
            eventChannel = null;
        }
    }

    public PendingTask<Snapshot> wait(WaitSnapEvent event) {
        return new SnapChannel(event);
    }
}
