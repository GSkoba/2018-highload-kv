package ru.mail.polis.gskoba;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class ValueSerializer {
    public static final ValueSerializer INSTANCE = new ValueSerializer();

    public byte[] serialize(@NotNull Value value) {
        int length = 20 + value.getData().length; //12 = long size + long size + int size
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putLong(value.getTimestamp());
        buffer.putLong(value.getTTL());
        buffer.putInt(value.getState().ordinal());
        buffer.put(value.getData());
        return buffer.array();
    }

    public Value deserialize(byte[] serializedValue) {
        ByteBuffer buffer = ByteBuffer.wrap(serializedValue);
        long timestamp = buffer.getLong();
        long ttl = buffer.getLong();
        int state = buffer.getInt();
        byte[] value = new byte[serializedValue.length - 20];
        buffer.get(value);
        return new Value(value, timestamp, ttl, state);
    }

}
