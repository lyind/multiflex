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

import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.format.Track;
import net.talpidae.multiflex.store.StoreException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;


public class BaseDescriptor implements Descriptor
{
    private final SQLiteTrack[] tracks;

    private final transient UUID storeId;

    private transient long id = 0;

    private transient int cachedHashCode = 0;


    private BaseDescriptor(SQLiteTrack[] tracks, long id, UUID storeId)
    {
        this.tracks = tracks;
        this.id = id;
        this.storeId = storeId;
    }

    /**
     * Build a descriptor out of the specified byte buffer.
     */
    static BaseDescriptor decode(ByteBuffer buffer, long id, UUID storeId) throws StoreException
    {
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        final int tracksLength = buffer.getInt();
        final int[] ids = Encoder.decodeIntegers(buffer, tracksLength, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
        final int[] encodings = Encoder.decodeIntegers(buffer, tracksLength, Encoding.INT32_VAR_BYTE_FAST_PFOR);

        final SQLiteTrack[] tracks = new SQLiteTrack[tracksLength];
        for (int i = 0; i < tracksLength; ++i)
        {
            final SQLiteTrack track = new SQLiteTrack(ids[i], Encoding.values.get(encodings[i]));
            track.setIndex(i);
            tracks[i] = track;
        }

        // we assume there is no weird behavior regarding this HeapByteBuffer's backing array
        return new BaseDescriptor(tracks, id, storeId);
    }

    @Override
    public boolean equals(Object other)
    {
        return this == other
                || (hashCode() == other.hashCode()
                && other instanceof BaseDescriptor
                && Objects.equals(storeId, ((BaseDescriptor) other).storeId)
                && Arrays.equals(tracks, ((BaseDescriptor) other).tracks));
    }

    @Override
    public int hashCode()
    {
        if (cachedHashCode == 0)
        {
            int code = 37 * 1007 + (int) id;

            code = 37 * code + storeId.hashCode();

            for (final Track track : tracks)
            {
                code = 37 * code + track.hashCode();
            }

            cachedHashCode = code;
        }

        return cachedHashCode;
    }

    /**
     * Clone this descriptor for another store instance.
     */
    BaseDescriptor forStore(UUID storeId)
    {
        if (!this.storeId.equals(storeId))
        {
            return new BaseDescriptor(Arrays.copyOf(tracks, tracks.length), 0L, storeId);
        }

        return this;
    }

    /**
     * Convert this descriptor to database representation.
     */
    ByteBuffer encode() throws StoreException
    {
        final int tracksLength = tracks.length;
        final int[] ids = new int[tracksLength];
        final int[] encodings = new int[tracksLength];
        int i = 0;
        for (final Track track : tracks)
        {
            ids[i] = track.getId();
            encodings[i] = track.getEncoding().ordinal();
            ++i;
        }

        final ByteBuffer buffer = ByteBuffer.allocate(tracksLength * 2 * 4).order(ByteOrder.LITTLE_ENDIAN);

        buffer.putInt(tracksLength); // length
        Encoder.encodeIntegers(ids, buffer, Encoding.INT32_DELTA_VAR_BYTE_FAST_PFOR);
        Encoder.encodeIntegers(encodings, buffer, Encoding.INT32_VAR_BYTE_FAST_PFOR);

        // we assume there is no weird behavior regarding this HeapByteBuffer's backing array
        return buffer;
    }


    /**
     * Get this descriptor's unique ID.
     *
     * @return ID assigned to this descriptor after it has been inserted into a store.
     */
    long getId()
    {
        return id;
    }


    void setId(long id)
    {
        this.id = id;
        this.cachedHashCode = 0;
    }


    @Override
    public Iterator<Track> iterator()
    {
        return new SQLiteTrackIterator();
    }

    @Override
    public int size()
    {
        return tracks.length;
    }

    @Override
    public SQLiteTrack getTrack(int trackId)
    {
        return binarySearchTrackById(trackId);
    }


    private SQLiteTrack binarySearchTrackById(int trackId)
    {
        int first = 0;
        int last = tracks.length - 1;

        while (first <= last)
        {
            final int middle = (first + last) >>> 1;

            final int value = tracks[middle].getId();
            if (value < trackId)
            {
                first = middle + 1;
            }
            else if (value > trackId)
            {
                last = middle - 1;
            }
            else
            {
                return tracks[middle];
            }
        }

        return null;
    }


    static class Builder implements Descriptor.Builder
    {
        private static final SQLiteTrack[] EMPTY_TRACKS = new SQLiteTrack[0];

        private final List<SQLiteTrack> tracks = new ArrayList<>();

        private final BaseStore store;


        Builder(BaseStore store)
        {
            this.store = store;
        }


        @Override
        public Descriptor.Builder track(int trackId, Encoding encoding)
        {
            if (encoding == null)
            {
                throw new NullPointerException("encoding is null");
            }

            tracks.add(new SQLiteTrack(trackId, encoding));

            return this;
        }

        @Override
        public Descriptor build()
        {
            final SQLiteTrack[] sortedTracks;
            if (tracks.isEmpty())
            {
                sortedTracks = EMPTY_TRACKS;
            }
            else
            {
                // stay compatible with Android
                //noinspection ComparatorCombinators
                tracks.sort((Track a, Track b) -> Integer.compare(a.getId(), b.getId()));

                // assign indices
                int index = 0;
                for (final SQLiteTrack track : tracks)
                {
                    track.setIndex(index);
                    ++index;
                }

                sortedTracks = tracks.toArray(new SQLiteTrack[index]);
            }

            final Descriptor descriptor = new BaseDescriptor(sortedTracks, 0L, store.getId());

            // clear builder for further use
            tracks.clear();
            return descriptor;
        }
    }


    static class SQLiteTrack implements Track
    {
        private final int id;

        private final Encoding encoding;

        private int index;


        private SQLiteTrack(int id, Encoding encoding)
        {
            this.id = id;
            this.encoding = encoding;
        }


        @Override
        public boolean equals(Object other)
        {
            return this == other
                    || (other instanceof SQLiteTrack
                    && id == ((SQLiteTrack) other).id
                    && encoding == ((SQLiteTrack) other).encoding);
        }


        @Override
        public int hashCode()
        {
            int code = 37 * 1007 + id;

            return 37 * code + encoding.ordinal();
        }


        int getIndex()
        {
            return index;
        }


        void setIndex(int index)
        {
            this.index = index;
        }


        @Override
        public int getId()
        {
            return id;
        }

        @Override
        public Encoding getEncoding()
        {
            return encoding;
        }
    }


    private class SQLiteTrackIterator implements Iterator<Track>
    {
        private int index = 0;


        @Override
        public boolean hasNext()
        {
            return index < tracks.length;
        }

        @Override
        public Track next()
        {
            final Track track = tracks[index];

            ++index;

            return track;
        }
    }
}
