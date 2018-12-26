package ru.mail.polis.gskoba;

import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final State state;
    private final Long TTL;

    enum State {
        PRESENT,
        DELETED,
        UNKNOWN
    }

    public Value(byte[] data) {
        this.data = data;
        this.timestamp = System.currentTimeMillis();
        this.state = State.PRESENT;
        this.TTL = Long.MAX_VALUE;
    }

    public Value(byte[] data, long timestamp, long ttl, int state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.values()[state];
        this.TTL = ttl;
    }

    public Value(byte[] data, long timestamp, long ttl, State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
        this.TTL = ttl;
    }

    public Value(byte[] data, long timestamp, long expireTime) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.PRESENT;
        this.TTL = expireTime;
    }

    public Value(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.PRESENT;
        this.TTL = null;
    }

    public State getState() {
        return state;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Long getTTL() {
        return TTL;
    }

    @Override
    public String toString() {
        return "Value{" +
                "data=" + Arrays.toString(data) +
                ", timestamp=" + timestamp +
                ", state=" + state +
                ", ttl=" + TTL +
                '}';
    }
}
