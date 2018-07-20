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
    private long pingInterval = 30;

    public long getPingInterval() {
        return pingInterval;
    }

    public RaaSOptions setPingInterval(long pingInterval) {
        if(pingInterval< this.inputTimeout) {
            this.pingInterval = pingInterval;
        }
        return this;
    }

    //timeout of receiving transfer data in seconds
    private long inputTimeout = 40;

    public long getInputTimeout() {
        return inputTimeout;
    }

    public RaaSOptions setInputTimeout(long inputTimeout) {
        this.inputTimeout = inputTimeout;
        return this;
    }

    //timeout of ping-pong
    private long pongTimeout = 5;

    public long getPongTimeout() {
        return pongTimeout;
    }

    public RaaSOptions setPongTimeout(long pongTimeout) {
        this.pongTimeout = pongTimeout;
        return this;
    }
}
