package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.transport.event.NodeID;
import io.lubricant.consensus.raft.transport.event.ShakeHandEvent;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * 事件节点（向其他节点发送事件）
 */
public class EventNode {

    private final static Logger logger = LoggerFactory.getLogger(EventNode.class);

    class EventChannel {

        volatile boolean ready;
        volatile Channel channel;

        public void connect() {
            new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
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
                                logger.info("Established channel with {}", dst);
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

    private final NodeID src, dst;
    private final NioEventLoopGroup group;
    private volatile EventChannel eventChannel;
    private volatile boolean shutdown;
    private volatile boolean initialized;

    public EventNode(NodeID src, NodeID dst, NioEventLoopGroup group) {
        this.src = src;
        this.dst = dst;
        this.group = group;
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
}
