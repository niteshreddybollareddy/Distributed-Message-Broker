package com.broker.replication;

import com.broker.core.BrokerDisruptor;
import com.broker.network.protocol.BrokerMessage;
import com.broker.network.protocol.FetchMessage;
import com.broker.network.protocol.FetchResponse;
import com.broker.network.protocol.MessageCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.atomic.AtomicLong;

public class ReplicaClient {
    private final String leaderHost;
    private final int leaderPort;
    private final BrokerDisruptor localDisruptor;
    private final String replicaId;
    
    private final AtomicLong nextFetchOffset = new AtomicLong(0);

    public ReplicaClient(String leaderHost, int leaderPort, BrokerDisruptor localDisruptor, String replicaId) {
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.localDisruptor = localDisruptor;
        this.replicaId = replicaId;
    }

    public void start() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new MessageCodec());
                    ch.pipeline().addLast(new ReplicaHandler());
                }
            });

            // Start the client.
            Channel channel = b.connect(leaderHost, leaderPort).sync().channel();
            
            // Send initial fetch request
            sendFetchRequest(channel);
            
            // Wait until the connection is closed.
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    private void sendFetchRequest(Channel channel) {
        FetchMessage fetch = new FetchMessage(replicaId, nextFetchOffset.get(), 1024 * 64);
        channel.writeAndFlush(fetch);
    }

    private class ReplicaHandler extends SimpleChannelInboundHandler<BrokerMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BrokerMessage msg) {
            if (msg instanceof FetchResponse) {
                FetchResponse response = (FetchResponse) msg;
                byte[] payloads = response.getPayloads();
                
                if (payloads.length > 0) {
                    // Publish to local disruptor
                    localDisruptor.publish(payloads);
                    // Update next offset (simplified: +1 per batch for our current logic)
                    nextFetchOffset.incrementAndGet();
                }
                
                // Immediately ask for more data
                sendFetchRequest(ctx.channel());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
