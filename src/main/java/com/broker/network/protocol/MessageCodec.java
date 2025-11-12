package com.broker.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageCodec extends ByteToMessageCodec<BrokerMessage> {

    private static final int MAGIC_NUMBER = 0xCAFEBABE;
    private static final int MIN_HEADER_LENGTH = 5; // 4 magic + 1 type

    @Override
    protected void encode(ChannelHandlerContext ctx, BrokerMessage msg, ByteBuf out) throws Exception {
        out.writeInt(MAGIC_NUMBER);
        out.writeByte(msg.getType());

        if (msg instanceof ProduceMessage) {
            ProduceMessage pm = (ProduceMessage) msg;
            out.writeInt(pm.getPayload().length);
            out.writeBytes(pm.getPayload());
        } else if (msg instanceof FetchMessage) {
            FetchMessage fm = (FetchMessage) msg;
            out.writeLong(fm.getOffset());
            out.writeInt(fm.getMaxBytes());
            byte[] groupIdBytes = fm.getConsumerGroupIdBytes();
            out.writeShort(groupIdBytes.length);
            out.writeBytes(groupIdBytes);
        } else if (msg instanceof FetchResponse) {
            FetchResponse fr = (FetchResponse) msg;
            out.writeLong(fr.getStartingOffset());
            out.writeInt(fr.getPayloads().length);
            out.writeBytes(fr.getPayloads());
        } else {
            throw new IllegalArgumentException("Unknown message type: " + msg.getClass());
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < MIN_HEADER_LENGTH) {
            return;
        }

        in.markReaderIndex();

        int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            in.resetReaderIndex();
            throw new IllegalStateException("Invalid magic number: " + Integer.toHexString(magic));
        }

        byte type = in.readByte();

        switch (type) {
            case BrokerMessage.TYPE_PRODUCE:
                if (in.readableBytes() < 4) { in.resetReaderIndex(); return; }
                int length = in.readInt();
                if (in.readableBytes() < length) { in.resetReaderIndex(); return; }
                byte[] payload = new byte[length];
                in.readBytes(payload);
                out.add(new ProduceMessage(payload));
                break;

            case BrokerMessage.TYPE_FETCH:
                if (in.readableBytes() < 14) { in.resetReaderIndex(); return; } // 8 offset + 4 max + 2 id_len
                long offset = in.readLong();
                int maxBytes = in.readInt();
                int idLen = in.readShort();
                if (in.readableBytes() < idLen) { in.resetReaderIndex(); return; }
                byte[] idBytes = new byte[idLen];
                in.readBytes(idBytes);
                String groupId = new String(idBytes, StandardCharsets.UTF_8);
                out.add(new FetchMessage(groupId, offset, maxBytes));
                break;

            case BrokerMessage.TYPE_FETCH_RESPONSE:
                if (in.readableBytes() < 12) { in.resetReaderIndex(); return; } // 8 offset + 4 length
                long startOffset = in.readLong();
                int payloadLen = in.readInt();
                if (in.readableBytes() < payloadLen) { in.resetReaderIndex(); return; }
                byte[] payloads = new byte[payloadLen];
                in.readBytes(payloads);
                out.add(new FetchResponse(startOffset, payloads));
                break;

            default:
                throw new IllegalStateException("Unknown message type: " + type);
        }
    }
}
