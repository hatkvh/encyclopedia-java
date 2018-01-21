package com.company;

import java.io.*;
import java.util.*;
public class Dictionary extends AbstractDictionary {
    /**
     * Hashtable which holds the in-memory index. For efficiency, the entire index
     * is cached in memory. The hashtable maps a key of type String to a PostHeader.
     */
    protected Hashtable index;
    /**
     * Creates a new database file.  The initialSize parameter determines the
     * amount of space which is allocated for the index.  The index can grow
     * dynamically, but the parameter is provide to increase
     * efficiency.
     */
    public Dictionary(String dbPath, int initialSize) throws IOException, DictionaryException {
        super(dbPath, initialSize);
        index = new Hashtable(initialSize);
    }
    /**
     * Opens an existing database and initializes the in-memory index.
     */
    public Dictionary(String dbPath, String accessFlags) throws IOException, DictionaryException {
        super(dbPath, accessFlags);
        int numRecords = readNumRecordsHeader();
        index = new Hashtable(numRecords);
        for (int i = 0; i < numRecords; i++) {
            String key = readKeyFromIndex(i);
            PostHeader header = readRecordHeaderFromIndex(i);
            header.setIndexPosition(i);
            index.put(key, header);
        }
    }
    /**
     * Returns an enumeration of all the keys in the database.
     */
    public synchronized Enumeration enumerateKeys() {
        return index.keys();
    }
    /**
     * Returns the current number of records in the database.
     */
    public synchronized int getNumRecords() {
        return index.size();
    }
    /**
     * Checks if there is a record belonging to the given key.
     */
    public synchronized boolean recordExists(String key) {
        return index.containsKey(key);
    }
    /**
     * Maps a key to a record header by looking it up in the in-memory index.
     */
    protected PostHeader keyToRecordHeader(String key) throws DictionaryException {
        PostHeader h = (PostHeader) index.get(key);
        if (h==null) {
            throw new DictionaryException("Key not found: " + key);
        }
        return h;
    }
    /**
     * This method searches the file for free space and then returns a PostHeader
     * which uses the space. (O(n) memory accesses)
     */
    protected PostHeader allocateRecord(String key, int dataLength) throws DictionaryException, IOException {
        // search for empty space
        PostHeader newRecord = null;
        Enumeration e = index.elements();
        while (e.hasMoreElements()) {
            PostHeader next = (PostHeader)e.nextElement();
            int free = next.getFreeSpace();
            if (dataLength <= next.getFreeSpace()) {
                newRecord = next.split();
                writeRecordHeaderToIndex(next);
                break;
            }
        }
        if (newRecord == null) {
            // append record to end of file - grows file to allocate space
            long fp = getFileLength();
            setFileLength(fp + dataLength);
            newRecord = new PostHeader(fp, dataLength);
        }
        return newRecord;
    }
    /**
     * Returns the record to which the target file pointer belongs - meaning the specified location
     * in the file is part of the record data of the PostHeader which is returned.  Returns null if
     * the location is not part of a record. (O(n) mem accesses)
     */
    protected PostHeader getRecordAt(long targetFp) throws DictionaryException {
        Enumeration e = index.elements();
        while (e.hasMoreElements()) {
            PostHeader next = (PostHeader) e.nextElement();
            if (targetFp >= next.dataPointer &&
                    targetFp < next.dataPointer + (long)next.dataCapacity) {
                return next;
            }
        }
        return null;
    }
    /**
     * Closes the database.
     */
    public synchronized void close() throws IOException, DictionaryException {
        try {
            super.close();
        } finally {
            index.clear();
            index = null;
        }
    }
    /**
     * Adds the new record to the in-memory index and calls the super class add
     * the index entry to the file.
     */
    protected void addEntryToIndex(String key, PostHeader newRecord, int currentNumRecords) throws IOException, DictionaryException {
        super.addEntryToIndex(key, newRecord, currentNumRecords);
        index.put(key, newRecord);
    }
    /**
     * Removes the record from the index. Replaces the target with the entry at the
     * end of the index.
     */
    protected void deleteEntryFromIndex(String key, PostHeader header, int currentNumRecords) throws IOException, DictionaryException {
        super.deleteEntryFromIndex(key, header, currentNumRecords);
        PostHeader deleted = (PostHeader) index.remove(key);
    }
}