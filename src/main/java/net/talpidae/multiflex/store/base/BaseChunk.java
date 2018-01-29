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

package net.talpidae.multiflex.store.base;

import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.base.BaseDescriptor.SQLiteTrack;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;


public class BaseChunk implements Chunk
{
    // TODO Optimize read access by utilizing SQLiteBlob

    private final BaseDescriptor descriptor;

    private final long timestamp;

    private final ByteBuffer data;

    private transient int[] offsets;

    private transient int[] lengths;

    // offset of first field (after index)
    private transient int fieldOffset = -1;


    BaseChunk(BaseDescriptor descriptor, long timestamp, ByteBuffer data)
    {
        this.descriptor = descriptor;
        this.timestamp = timestamp;
        this.data = data.order(ByteOrder.LITTLE_ENDIAN);
    }


    @Override
    public BaseDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public long getTimestamp()
    {
        return timestamp;
    }


    /**
     * Return this Chunk or a representation suitable for use with the specified store ID.
     * <p>
     * The associated descriptor is also made local to the store.
     */
    BaseChunk forStore(UUID storeId)
    {
        final BaseDescriptor localDescriptor = descriptor.forStore(storeId);
        if (localDescriptor != descriptor)
        {
            return new BaseChunk(localDescriptor, timestamp, data);
        }

        return this;
    }


    /**
     * Decompress index if necessary.
     */
    private void decompressIndex() throws StoreException
    {
        final int indexSize = descriptor.size();
        final ByteBuffer indexData = data.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        indexData.position(0);
        indexData.limit(Math.min(indexSize * 8 + (indexSize * 4), indexData.remaining()));

        // TODO Fix decompression issue here
        offsets = Encoder.decodeIntegers(indexData, indexSize, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
        lengths = Encoder.decodeIntegers(indexData, indexSize, Encoding.INT32_VAR_BYTE_FAST_PFOR);

        fieldOffset = indexData.position();
    }


    /**
     * Locate the track's data and adjust the data buffers position and limit to the field's bounds.
     */
    private SQLiteTrack locateTrack(int streamId) throws StoreException
    {
        final SQLiteTrack track = descriptor.getTrack(streamId);
        if (track != null)
        {
            if (fieldOffset < 0)
            {
                decompressIndex();
            }

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

            return Encoder.decodeIntegers(getField(track.getIndex()), length, track.getEncoding());
        }

        return null;
    }


    @Override
    public String getText(int streamId) throws StoreException
    {
        final SQLiteTrack track = locateTrack(streamId);
        if (track != null)
        {
            return Encoder.decodeText(getField(track.getIndex()), track.getEncoding());
        }

        return null;
    }


    @Override
    public ByteBuffer getBinary(int streamId) throws StoreException
    {
        final SQLiteTrack track = locateTrack(streamId);
        if (track != null)
        {
            return Encoder.decodeBinary(getField(track.getIndex()), track.getEncoding());
        }

        return null;
    }


    public ByteBuffer getData()
    {
        return data.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }


    @Override
    public void close() throws Exception
    {
        fieldOffset = -1;
        offsets = null;
        lengths = null;
    }


    private ByteBuffer getField(int index)
    {
        final int nextIndex = index + 1;
        final int position = fieldOffset + offsets[index];

        final ByteBuffer field = data.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        field.position(position);
        field.limit(nextIndex < offsets.length ? fieldOffset + offsets[nextIndex] : field.limit());

        return field;
    }


    static class Builder implements Chunk.Builder
    {
        private final BaseDescriptor descriptor;

        private final ByteBuffer[] values;

        private final int[] uncompressedLengths;

        private long timestamp = -1;


        Builder(BaseDescriptor descriptor)
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

            // create copy of array, because lower methods modify it
            final int[] integersCopy = Arrays.copyOf(integers, integers.length);

            final ByteBuffer data = ByteBuffer.allocate(uncompressedLength * 8).order(ByteOrder.LITTLE_ENDIAN);
            Encoder.encodeIntegers(integersCopy, data, track.getEncoding());
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

            final ByteBuffer data = ByteBuffer.allocate(uncompressedLength * 4).order(ByteOrder.LITTLE_ENDIAN);
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
            final ByteBuffer data = ByteBuffer.allocate(uncompressedBytes).order(ByteOrder.LITTLE_ENDIAN);
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


        /**
         * Reset this builder so it can be re-used to build another chunk.
         */
        @Override
        public void reset()
        {
            // prepare for re-use
            this.timestamp = -1;
            Arrays.fill(this.values, null);
            Arrays.fill(this.uncompressedLengths, 0);
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

            // over-allocate by a few bytes (don't know offsets/lengths compressed size, yet)
            final ByteBuffer data = ByteBuffer.allocate((index * 8) + compressedDataTotal).order(ByteOrder.LITTLE_ENDIAN);

            // we always know how many integers we have uncompressed from the descriptor
            Encoder.encodeIntegers(offsets, data, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
            Encoder.encodeIntegers(uncompressedLengths, data, Encoding.INT32_VAR_BYTE_FAST_PFOR);

            // start writing data behind offsets and uncompressed lengths
            for (final ByteBuffer value : values)
            {
                data.put(value);
            }

            data.flip();

            final Chunk chunk = new BaseChunk(descriptor, timestamp, data);

            // prepare for re-use
            reset();

            return chunk;
        }
    }
}
