package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.transport.event.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事件总线（接收其他节点发送的事件）
 */
public class EventBus extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(EventBus.class);

    public interface EventDispatcher {
        void on(PingEvent event);
        void on(PongEvent event);
        void on(OneWayEvent event);
    }

    private final int port;
    private final EventDispatcher dispatcher;
    private Channel channel;

    public EventBus(int port, EventDispatcher dispatcher) {
        super("EventBus-" + port);
        this.port = port;
        this.dispatcher = dispatcher;
    }

    private class DisconnectedHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Uncaught exception in handler", cause);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Node channel closed");
        }
    }

    private class ShakeHandHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ShakeHandEvent) {
                try {
                    String message = ((ShakeHandEvent) msg).message();
                    NodeID nodeID = NodeID.fromString(message);
                    ctx.pipeline().replace(
                            EventCodec.STR_EVENT_DECODER,
                            EventCodec.BIN_EVENT_DECODER,
                            EventCodec.binEventDecoder(nodeID));
                    ctx.pipeline().addLast(new EventDispatchHandler(nodeID));
                    ctx.writeAndFlush(ShakeHandEvent.ok());
                } catch (Exception e) {
                    logger.error("Shake hand failed", e);
                    ctx.writeAndFlush(new ShakeHandEvent(e.getMessage()));
                    ctx.close();
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

    }

    private class EventDispatchHandler extends ChannelInboundHandlerAdapter {

        private final NodeID nodeID;

        EventDispatchHandler(NodeID nodeID) {
            this.nodeID = nodeID;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof Event.BinEvent) {
                EventID src = ((Event.BinEvent) msg).source();
                if (src.nodeID().equals(nodeID)) {
                    if (msg instanceof OneWayEvent) {
                        dispatcher.on((OneWayEvent) msg);
                    } else if (msg instanceof PingEvent) {
                        dispatcher.on((PingEvent) msg);
                    } else if (msg instanceof PongEvent) {
                        dispatcher.on((PongEvent) msg);
                    } else {
                        logger.error("Unexpected event {}", msg);
                    }
                } else {
                    throw new IllegalArgumentException(
                            String.format("nodeID mismatch: %s != %s", src.nodeID(), nodeID));
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }

    @Override
    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(EventCodec.frameDecoder())
                                    .addLast(EventCodec.STR_EVENT_DECODER, EventCodec.strEventDecoder())
                                    .addLast(EventCodec.frameEncoder())
                                    .addLast(EventCodec.eventEncoder())
                                    .addLast(new ShakeHandHandler())
                                    .addLast(new DisconnectedHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture f = b.bind(port).addListener(new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("EventBus is ready");
                }
            }).sync();
            channel = f.channel();
            channel.closeFuture().sync();
        } catch (InterruptedException ignored) {
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
    }

}
