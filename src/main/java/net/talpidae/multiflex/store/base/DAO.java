package net.talpidae.multiflex.store.base;

import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.base.Transaction;

import java.nio.ByteBuffer;
import java.util.List;


public interface DAO extends AutoCloseable
{
    /**
     * Get a transaction control object that represents an active transaction.
     */
    Transaction getTransaction() throws StoreException;


    /**
     * Open the associated file and prepare this DAO instance for further operations.
     */
    void open(boolean writable) throws StoreException;


    /**
     * Perform the specified migration.
     */
    void execMigration(int currentVersion, int targetVersion, String sqlMigration) throws StoreException;


    /**
     * Query the schema version. Not cached since it is rarely needed.
     * <p>
     * This also checks if the meta table already exists and returns schema version 0 otherwise.
     */
    int selectVersion() throws StoreException;

    /**
     * Query the schema version. Not cached since it is rarely needed.
     */
    String selectMeta(String key) throws StoreException;

    /**
     * Insert or replace a value for the meta field with the specified key.
     */
    void insertOrReplaceMeta(String key, String value) throws StoreException;

    /**
     * Insert descriptor if not exists and select its ID.
     */
    long insertDescriptorAndSelectId(ByteBuffer descriptor) throws StoreException;

    /**
     * Get a descriptors data by ID.
     */
    ByteBuffer selectDescriptor(long id) throws StoreException;

    /**
     * Find the maximum track chunk timestamp.
     *
     * @return The maximum timestamp of any chunk contained in this store. -1 if no timestamp has been found (no chunks).
     */
    long selectMaxChunkTimestamp() throws StoreException;

    /**
     * Insert a track chunk.
     */
    void insertOrReplaceTrackChunk(long timestamp, long descriptorId, ByteBuffer data) throws StoreException;

    /**
     * Find all chunks that lie within the specified timestamp (seconds since epochMillies in table meta).
     * <p>
     * Call this within a transaction.
     *
     * @param tsBegin        The begin of the range (inclusive)
     * @param tsEnd         The upper limit of the range (exclusive)
     * @param descriptorById Function that get a descriptor by the specified descriptor ID
     * @return A list of Chunks constructed from the located DB entries, empty list if none were found
     */
    List<Chunk> selectChunksByTimestampRange(long tsBegin, long tsEnd, ChunkFactory descriptorById) throws StoreException;

    /**
     * Find a chunk by timestamp.
     * <p>
     * Call this within a transaction.
     */
    Chunk selectChunkByTimestamp(long timestamp, ChunkFactory descriptorById) throws StoreException;


    @FunctionalInterface
    interface ChunkFactory
    {
        /**
         * Find a descriptor by ID and construct a chunk object from the specified timestamp and data.
         * <p>
         * Must throw a StoreException in case the chunk can't be built successfully.
         */
        Chunk createChunk(long timestamp, long descriptorId, ByteBuffer data) throws StoreException;
    }
}
