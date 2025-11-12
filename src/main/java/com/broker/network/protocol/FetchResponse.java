package com.broker.network.protocol;

public class FetchResponse implements BrokerMessage {
    private final long startingOffset;
    private final byte[] payloads; // Batch of payloads concatenated or similar, for simplicity just raw bytes here

    public FetchResponse(long startingOffset, byte[] payloads) {
        this.startingOffset = startingOffset;
        this.payloads = payloads;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public byte[] getPayloads() {
        return payloads;
    }

    @Override
    public byte getType() {
        return TYPE_FETCH_RESPONSE;
    }
}
