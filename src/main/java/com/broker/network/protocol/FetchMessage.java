package com.broker.network.protocol;

import java.nio.charset.StandardCharsets;

public class FetchMessage implements BrokerMessage {
    private final String consumerGroupId;
    private final long offset;
    private final int maxBytes;

    public FetchMessage(String consumerGroupId, long offset, int maxBytes) {
        this.consumerGroupId = consumerGroupId;
        this.offset = offset;
        this.maxBytes = maxBytes;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public byte[] getConsumerGroupIdBytes() {
        return consumerGroupId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte getType() {
        return TYPE_FETCH;
    }
}
