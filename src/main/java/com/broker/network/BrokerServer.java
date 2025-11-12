package com.broker.network;

import com.broker.core.BrokerDisruptor;
import com.broker.network.protocol.BrokerMessage;
import com.broker.network.protocol.FetchMessage;
import com.broker.network.protocol.FetchResponse;
import com.broker.network.protocol.MessageCodec;
import com.broker.network.protocol.ProduceMessage;
import com.broker.storage.CommitLog;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class BrokerServer {

    private final int port;
    private final BrokerDisruptor disruptor;
    private final CommitLog commitLog;

    public BrokerServer(int port, BrokerDisruptor disruptor, CommitLog commitLog) {
        this.port = port;
        this.disruptor = disruptor;
        this.commitLog = commitLog;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new MessageCodec());
                     ch.pipeline().addLast(new BrokerMessageHandler(disruptor, commitLog));
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 128)          
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("BrokerServer started on port " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private static class BrokerMessageHandler extends SimpleChannelInboundHandler<BrokerMessage> {
        private final BrokerDisruptor disruptor;
        private final CommitLog commitLog;

        public BrokerMessageHandler(BrokerDisruptor disruptor, CommitLog commitLog) {
            this.disruptor = disruptor;
            this.commitLog = commitLog;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BrokerMessage msg) {
            if (msg instanceof ProduceMessage) {
                ProduceMessage pm = (ProduceMessage) msg;
                disruptor.publish(pm.getPayload());
            } else if (msg instanceof FetchMessage) {
                FetchMessage fm = (FetchMessage) msg;
                // Ideally this read would happen on a separate I/O thread pool to not block Netty event loop,
                // but we keep it here for simplified mechanical sympathy implementation
                byte[] payloads = commitLog.readBatch(fm.getOffset(), fm.getMaxBytes());
                
                // Send response
                FetchResponse response = new FetchResponse(fm.getOffset(), payloads);
                ctx.writeAndFlush(response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
