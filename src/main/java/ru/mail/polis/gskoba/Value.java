package ru.mail.polis.gskoba;

import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final State state;
    private final long TTL;

    enum State {
        PRESENT,
        DELETED,
        UNKNOWN
    }

    public Value(byte[] data) {
        this.data = data;
        this.timestamp = System.currentTimeMillis();
        this.state = State.PRESENT;
        this.TTL = 86400000;
    }

    public Value(byte[] data, long timestamp, int state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.values()[state];
        this.TTL = 86400000;
    }

    public Value(byte[] data, long timestamp, State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
        this.TTL = 86400000;
    }

    public Value(byte[] data, long timestamp, long expireTime) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.PRESENT;
        this.TTL = expireTime;
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

    public long getTTL() {
        return TTL;
    }

    @Override
    public String toString() {
        return "Value{" +
                "data=" + Arrays.toString(data) +
                ", timestamp=" + timestamp +
                ", state=" + state +
                '}';
    }
}
