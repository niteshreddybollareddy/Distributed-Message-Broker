package com.broker.network.protocol;

public class ProduceMessage implements BrokerMessage {
    private final byte[] payload;

    public ProduceMessage(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public byte getType() {
        return TYPE_PRODUCE;
    }
}
