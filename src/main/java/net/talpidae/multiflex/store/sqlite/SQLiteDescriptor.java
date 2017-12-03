package net.talpidae.multiflex.store.sqlite;

import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.format.Encoding;
import net.talpidae.multiflex.format.Track;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;


public class SQLiteDescriptor implements Descriptor
{
    private final SQLiteTrack[] tracks;

    private final transient UUID storeId;

    private transient long id = 0;

    private transient int cachedHashCode = 0;


    SQLiteDescriptor(SQLiteTrack[] tracks, long id, UUID storeId)
    {
        this.tracks = tracks;
        this.id = id;
        this.storeId = storeId;
    }


    @Override
    public boolean equals(Object other)
    {
        return this == other
                || (hashCode() == other.hashCode()
                && other instanceof SQLiteDescriptor
                && Objects.equals(storeId, ((SQLiteDescriptor) other).storeId)
                && Arrays.equals(tracks, ((SQLiteDescriptor) other).tracks));
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
    SQLiteDescriptor forStore(UUID storeId)
    {
        if (!this.storeId.equals(storeId))
        {
            return new SQLiteDescriptor(Arrays.copyOf(tracks, tracks.length), 0L, storeId);
        }

        return this;
    }


    /**
     * Convert this descriptor to database representation.
     */
    byte[] encode()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(tracks.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        for (final Track track : tracks)
        {
            buffer.putInt(track.getId());
            buffer.putInt(track.getEncoding().ordinal());
        }

        // we assume there is no weird behavior regarding this HeapByteBuffer's backing array
        return buffer.array();
    }


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
        return tracks.length / 2;
    }

    @Override
    public SQLiteTrack getTrack(int trackId)
    {
        return binarySearchTrackById(trackId);
    }


    private SQLiteTrack binarySearchTrackById(int trackId)
    {
        int first = 0;
        int last = (tracks.length >>> 2) - 1;

        while (first <= last)
        {
            final int middle = (first + last) >>> 1;

            final int value = tracks[middle << 1].getId();
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
                return tracks[middle << 2];
            }
        }

        return null;
    }


    static class Builder implements Descriptor.Builder
    {
        private static final SQLiteTrack[] EMPTY_TRACKS = new SQLiteTrack[0];

        private final List<SQLiteTrack> tracks = new ArrayList<>();

        private final SQLiteStore store;


        Builder(SQLiteStore store)
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

            return new SQLiteDescriptor(sortedTracks, 0L, store.getId());
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
