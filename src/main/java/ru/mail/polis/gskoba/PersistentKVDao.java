package ru.mail.polis.gskoba;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentKVDao implements KVDao {
    private final DB db;
    private final File data;
    private final HTreeMap<byte[], byte[]> storage;

    public PersistentKVDao(File directory) {
        this.data = new File(directory, "db");
        this.db = DBMaker.fileDB(data).fileChannelEnable()
                .fileMmapPreclearDisable().fileMmapEnable()
                .fileMmapEnableIfSupported().make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final byte[] out = this.storage.get(key);
        if (out == null) throw new NoSuchElementException();
        return out;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        this.storage.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        this.storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    public long cleanUp(long interval, final TimeUnit timeUnit) {
        AtomicLong atomicLong = new AtomicLong();
        storage.entrySet().removeIf( e -> {
            Map.Entry<byte[], byte[]> entry = (Map.Entry)e;
            Value value = ValueSerializer.INSTANCE.deserialize(entry.getValue());
            if (System.currentTimeMillis() - value.getTTL() >= timeUnit.toMillis(interval)) {
                atomicLong.incrementAndGet();
                return true;
            }
            return false;
        });
        return atomicLong.get();
    }
}
