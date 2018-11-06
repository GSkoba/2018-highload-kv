package ru.mail.polis.gskoba;

import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {

    private byte[] data;
    private long timestamp;
    private State state;

    enum State{
        PRESENT,
        DELETED,
        UNKNOWN
    }

    public Value(byte[] data, long timestamp, int state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.values()[state];
    }

    public Value(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.PRESENT;
    }

    public State getState() {
        return state;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
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
