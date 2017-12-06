/*
 * Copyright (C) 2017  Jonas Zeiger <jonas.zeiger@talpidae.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLiteException;
import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.sqlite.SQLiteDescriptor.SQLiteTrack;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


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
        private final SQLiteDescriptor descriptor;

        private final ByteBuffer[] values;

        private final int[] uncompressedLengths;

        private long timestamp = -1;


        Builder(SQLiteDescriptor descriptor)
        {
            this.descriptor = descriptor;

            final int trackCount = descriptor.size();
            this.values = new ByteBuffer[trackCount];
            this.uncompressedLengths = new int[trackCount];
        }

        @Override
        public Chunk.Builder timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public Chunk.Builder integers(int trackId, int[] integers) throws StoreException
        {
            final SQLiteTrack track = findTrack(trackId);

            final int uncompressedLength = integers.length;

            final ByteBuffer data = ByteBuffer.allocate(uncompressedLength);
            Encoder.encodeIntegers(integers, data, track.getEncoding());
            data.flip();

            // store uncompressed length to allow for decompression
            final int index = track.getIndex();
            if (values[index] == null)
            {
                values[index] = data;
                uncompressedLengths[index] = uncompressedLength;
            }
            else
            {
                throw new IllegalArgumentException("values for track with id " + trackId + " have already been set");
            }

            return this;
        }

        @Override
        public Chunk.Builder text(int trackId, String text) throws StoreException
        {
            final SQLiteTrack track = findTrack(trackId);

            final int uncompressedLength = text.length();

            final ByteBuffer data = ByteBuffer.allocate(uncompressedLength * 4);
            Encoder.encodeText(text, data, track.getEncoding());
            data.flip();

            final int index = track.getIndex();
            if (values[index] == null)
            {
                values[index] = data;
                uncompressedLengths[index] = uncompressedLength;
            }
            else
            {
                throw new IllegalArgumentException("values for track with id " + trackId + " have already been set");
            }

            return this;
        }

        @Override
        public Chunk.Builder binary(int trackId, ByteBuffer binary)
        {
            final SQLiteTrack track = findTrack(trackId);

            final int uncompressedBytes = binary.remaining();

            // we may later use a real compression scheme, so don't just put the original buffer
            final ByteBuffer data = ByteBuffer.allocate(uncompressedBytes);
            Encoder.encodeBinary(binary, data, track.getEncoding());
            data.flip();

            final int index = track.getIndex();
            if (values[index] == null)
            {
                values[index] = data;
                uncompressedLengths[index] = uncompressedBytes;
            }
            else
            {
                throw new IllegalArgumentException("values for track with id " + trackId + " have already been set");
            }

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


        @Override
        public Chunk build() throws StoreException
        {
            if (timestamp < 0)
            {
                throw new IllegalArgumentException("timestamp not set or invalid");
            }

            // calculate offsets and total compressed data length
            final int[] offsets = new int[values.length];
            int compressedDataTotal = 0;
            int index = 0;
            for (final ByteBuffer value : values)
            {
                offsets[index] = compressedDataTotal;

                compressedDataTotal += value != null ? value.remaining() : 0;
                ++index;
            }

            // over-allocate by just a few bytes (don't know offsets/lengths compressed size, yet)
            final ByteBuffer data = ByteBuffer.allocate((index * 2) + compressedDataTotal)
                    .order(ByteOrder.LITTLE_ENDIAN);

            // we always know how many integers we have uncompressed from the descriptor
            Encoder.encodeIntegers(offsets, data, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
            Encoder.encodeIntegers(uncompressedLengths, data, Encoding.INT32_VAR_BYTE_FAST_PFOR);

            // start writing data behind offsets and uncompressed lengths
            for (final ByteBuffer value : values)
            {
                data.put(value);
            }

            return new SQLiteChunk(descriptor, timestamp, data);
        }
    }
}
