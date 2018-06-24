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
}
