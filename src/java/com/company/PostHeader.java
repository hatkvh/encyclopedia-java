package com.company;
import java.io.*;
public class PostHeader {
    /**
     * File pointer to the first byte of record data (8 bytes).
     */
    protected long dataPointer;
    /**
     * Actual number of bytes of data held in this record (4 bytes).
     */
    protected int dataCount;
    /**
     * Number of bytes of data that this record can hold (4 bytes).
     */
    protected int dataCapacity;
    /**
     * Indicates this header's position in the file index.
     */
    protected int indexPosition;
    protected PostHeader() {
    }
    protected PostHeader(long dataPointer, int dataCapacity) {
        if (dataCapacity < 1) {
            throw new IllegalArgumentException("Bad record size: " + dataCapacity);
        }
        this.dataPointer = dataPointer;
        this.dataCapacity = dataCapacity;
        this.dataCount = 0;
    }
    protected int getIndexPosition() {
        return indexPosition;
    }
    protected void setIndexPosition(int indexPosition) {
        this.indexPosition = indexPosition;
    }
    protected int getDataCapacity() {
        return dataCapacity;
    }
    protected int getFreeSpace() {
        return dataCapacity - dataCount;
    }
    protected void read(DataInput in) throws IOException {
        dataPointer = in.readLong();
        dataCapacity = in.readInt();
        dataCount = in.readInt();
    }
    protected void write(DataOutput out) throws IOException {
        out.writeLong(dataPointer);
        out.writeInt(dataCapacity);
        out.writeInt(dataCount);
    }
    protected static PostHeader readHeader(DataInput in) throws IOException {
        PostHeader r = new PostHeader();
        r.read(in);
        return r;
    }
    /**
     * Returns a new record header which occupies the free space of this record.
     * Shrinks this record size by the size of its free space.
     */
    protected PostHeader split() throws DictionaryException {
        long newFp = dataPointer + (long)dataCount;
        PostHeader newRecord = new PostHeader(newFp, getFreeSpace());
        dataCapacity = dataCount;
        return newRecord;
    }
}