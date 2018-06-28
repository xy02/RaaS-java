package com.github.xy02.raas.nats;

import com.github.xy02.nats.Options;

public class RaaSOptions {
    //use only single socket
    private boolean singleSocket = false;

    public boolean isSingleSocket() {
        return singleSocket;
    }

    public RaaSOptions setSingleSocket(boolean singleSocket) {
        this.singleSocket = singleSocket;
        return this;
    }

    //nats-rx options
    private Options natsOptions = new Options();

    public Options getNatsOptions() {
        return natsOptions;
    }

    public RaaSOptions setNatsOptions(Options natsOptions) {
        this.natsOptions = natsOptions;
        return this;
    }

    //interval of service sending ping in seconds
    private long pingInterval = 40;

    public long getPingInterval() {
        return pingInterval;
    }

    public RaaSOptions setPingInterval(long pingInterval) {
        this.pingInterval = pingInterval;
        return this;
    }

    //timeout of receiving transfer data in seconds
    private long inputTimeout = 90;

    public long getInputTimeout() {
        return inputTimeout;
    }

    public RaaSOptions setInputTimeout(long inputTimeout) {
        this.inputTimeout = inputTimeout;
        return this;
    }

    //timeout of handshake of calling service in seconds(equals to timeout of ping pong)
    private long handshakeTimeout = 5;

    public long getHandshakeTimeout() {
        return handshakeTimeout;
    }

    public RaaSOptions setHandshakeTimeout(long handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;
        return this;
    }
}
