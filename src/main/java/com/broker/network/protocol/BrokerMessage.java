package com.broker.network.protocol;

public interface BrokerMessage {
    byte TYPE_PRODUCE = 1;
    byte TYPE_FETCH = 2;
    byte TYPE_FETCH_RESPONSE = 3;

    byte getType();
}
