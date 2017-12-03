package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLiteException;
import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.format.Track;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.sqlite.SQLiteDescriptor.SQLiteTrack;
import net.talpidae.multiflex.store.util.SparseArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;


public class SQLiteChunk implements Chunk
{
    // TODO Optimize read access by utilizing SQLiteBlob

    private final SQLiteDescriptor descriptor;

    private final long timestamp;

    private final ByteBuffer data;

    private int[] offsets;

    private int[] lengths;


    SQLiteChunk(SQLiteDescriptor descriptor, long timestamp, ByteBuffer data)
    {
        this.descriptor = descriptor;
        this.timestamp = timestamp;
        this.data = data;
    }


    @Override
    public SQLiteDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public long getTimestamp()
    {
        return timestamp;
    }


    /**
     * Decompress index if necessary.
     */
    private void decompressIndex() throws StoreException
    {
        data.position(0);
        offsets = Encoder.decodeIntegers(data, descriptor.size(), Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
        lengths = Encoder.decodeIntegers(data, descriptor.size(), Encoding.INT32_VAR_BYTE_FAST_PFOR);
    }


    /**
     * Locate the track's data and adjust the data buffers position and limit to the field's bounds.
     */
    private SQLiteTrack locateTrack(int streamId) throws StoreException
    {
        final SQLiteTrack track = descriptor.getTrack(streamId);
        if (track != null)
        {
            if (offsets == null || lengths == null)
            {
                decompressIndex();
            }

            final int index = track.getIndex();

            final int offset = offsets[index];
            data.position(offset);

            final int nextOffset = (index + 1) < offsets.length ? offsets[index] : data.limit();
            data.limit(nextOffset);

            return track;
        }

        return null;
    }


    @Override
    public int[] getIntegers(int streamId) throws StoreException
    {
        final SQLiteTrack track = locateTrack(streamId);
        if (track != null)
        {
            final int length = lengths[track.getIndex()];

            return Encoder.decodeIntegers(data, length, track.getEncoding());
        }

        return null;
    }

    @Override
    public String getText(int streamId) throws StoreException
    {
        final SQLiteTrack track = locateTrack(streamId);
        if (track != null)
        {
            return Encoder.decodeText(data, track.getEncoding());
        }

        return null;
    }


    @Override
    public ByteBuffer getBinary(int streamId) throws StoreException
    {
        final SQLiteTrack track = locateTrack(streamId);
        if (track != null)
        {
            return Encoder.decodeBinary(data, track.getEncoding());
        }

        return null;
    }


    /**
     * Write this chunk out to the specified output stream.
     */
    void persist(DAO dao) throws StoreException
    {
        try
        {
            dao.insertOrReplaceTrackChunk(timestamp, descriptor, data);
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to persist chunk", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        offsets = null;
        lengths = null;
    }


    static class Builder implements Chunk.Builder
    {
        private final SparseArray<Object> values = new SparseArray<>();

        private final SQLiteDescriptor descriptor;

        private final int[] offsets;

        private final int[] lengths;

        private long timestamp = -1;


        Builder(SQLiteDescriptor descriptor)
        {
            this.descriptor = descriptor;
            this.offsets = new int[descriptor.size()];
            this.lengths = new int[descriptor.size()];
        }

        @Override
        public Chunk.Builder timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public Chunk.Builder integers(int trackId, int[] integers)
        {
            final SQLiteTrack track = findTrack(trackId);

            this.values.put(trackId, integers);
            return this;
        }

        @Override
        public Chunk.Builder text(int trackId, String text)
        {
            this.values.put(trackId, text);
            return this;
        }

        @Override
        public Chunk.Builder binary(int trackId, ByteBuffer binary)
        {
            this.values.put(trackId, binary);
            return this;
        }


        private SQLiteTrack findTrack(int trackId)
        {
            final SQLiteTrack track = descriptor.getTrack(trackId);
            if (track == null)
            {
                throw new IllegalArgumentException("specified track is not registered with descriptor: " + trackId);
            }

            return track;
        }


        private int estimateDataLength()
        {
            // values (uncompressed)
            return values.reduce((sum, key, value) ->
            {
                if (value instanceof int[])
                {
                    return sum += ((int[]) value).length * 4;
                }
                else if (value instanceof ByteBuffer)
                {
                    return sum += ((ByteBuffer) value).remaining();
                }
                else if (value instanceof String)
                {
                    return sum += ((String) value).length() * 2;
                }
                else
                {
                    // null or unsupported
                    return sum += 0;
                }
            }, 0);
        }


        @Override
        public Chunk build()
        {
            // n offsets @ 4byte
            final int offsetsLength = descriptor.size() * 4;
            final ByteBuffer data = ByteBuffer.allocate(offsetsLength + estimateDataLength())
                    .order(ByteOrder.LITTLE_ENDIAN);

            // start writing data behind offset list
            data.position(offsetsLength);

            int offsetPosition = 0;
            for (final Track track : descriptor)
            {
                // write offset
                data.putInt(offsetPosition, data.position());
                offsetPosition += 4;

                final Object value = values.get(track.getId());
                if (value instanceof int[])
                {
                    // compress
                    //return sum += ((int[]) value).length * 4;
                }
                else if (value instanceof ByteBuffer)
                {
                    data.put((ByteBuffer) value);
                }
                else if (value instanceof String)
                {
                    data.put(((String) value).getBytes(StandardCharsets.UTF_8));
                }
            }

            return new SQLiteChunk(descriptor, timestamp, data);
        }
    }
}
