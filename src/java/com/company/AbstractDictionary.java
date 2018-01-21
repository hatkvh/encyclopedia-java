package com.company;
import java.io.*;
import java.util.*;
public abstract class AbstractDictionary {
    // The database file.
    private RandomAccessFile file;
    // Current file pointer to the start of the record data.
    protected long dataStartPtr;
    // Total length in bytes of the global database headers.
    protected static final int FILE_HEADERS_REGION_LENGTH = 16;
    // Number of bytes in the record header.
    protected static final int RECORD_HEADER_LENGTH = 16;
    // The length of a key in the index. 256 bytes will ensure max key length is 1024 characters
    protected static final int MAX_KEY_LENGTH = 256;
    // The total length of one index entry - the key length plus the record header length.
    protected static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH + RECORD_HEADER_LENGTH;
    // File pointer to the num records header.
    protected static final long NUM_RECORDS_HEADER_LOCATION = 0;
    // File pointer to the data start pointer header.
    protected static final long DATA_START_HEADER_LOCATION = 4;

    protected AbstractDictionary(String dbPath, int initialSize) throws IOException, DictionaryException {
        File f = new File(dbPath);
        if (f.exists()) {
            throw new DictionaryException("Database already exits: " + dbPath);
        }
        file = new RandomAccessFile(f, "rw");
        dataStartPtr = indexPositionToKeyFp(initialSize);
        setFileLength(dataStartPtr);
        writeNumRecordsHeader(0);
        writeDataStartPtrHeader(dataStartPtr);
    }

    protected AbstractDictionary(String dbPath, String accessFlags) throws IOException, DictionaryException {
        File f = new File (dbPath);
        if(!f.exists()) {
            throw new DictionaryException("Database not found: " + dbPath);
        }
        file = new RandomAccessFile(f, accessFlags);
        dataStartPtr = readDataStartHeader();
    }

    public abstract Enumeration enumerateKeys();

    public abstract int getNumRecords();

    public abstract boolean recordExists(String key);

    protected abstract PostHeader keyToRecordHeader(String key) throws DictionaryException;

    protected abstract PostHeader allocateRecord(String key, int dataLength) throws DictionaryException, IOException;

    protected abstract PostHeader getRecordAt(long targetFp) throws DictionaryException;
    protected long getFileLength() throws IOException {
        return file.length();
    }
    protected void setFileLength(long l) throws IOException {
        file.setLength(l);
    }

    protected int readNumRecordsHeader() throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        return file.readInt();
    }

    protected void writeNumRecordsHeader(int numRecords) throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        file.writeInt(numRecords);
    }

    protected long readDataStartHeader() throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        return file.readLong();
    }

    protected void writeDataStartPtrHeader(long dataStartPtr) throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        file.writeLong(dataStartPtr);
    }

    protected long indexPositionToKeyFp(int pos) {
        return FILE_HEADERS_REGION_LENGTH + (INDEX_ENTRY_LENGTH * pos);
    }

    long indexPositionToRecordHeaderFp(int pos) {
        return indexPositionToKeyFp(pos) + MAX_KEY_LENGTH;
    }

    String readKeyFromIndex(int position) throws IOException {
        file.seek(indexPositionToKeyFp(position));
        return file.readUTF();
    }

    PostHeader readRecordHeaderFromIndex(int position) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(position));
        return PostHeader.readHeader(file);
    }

    protected void writeRecordHeaderToIndex(PostHeader header) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(header.indexPosition));
        header.write(file);
    }

    protected void addEntryToIndex(String key, PostHeader newRecord, int currentNumRecords) throws IOException, DictionaryException {
        CustomByteArrayOutputStream temp = new CustomByteArrayOutputStream(MAX_KEY_LENGTH);
        (new DataOutputStream(temp)).writeUTF(key);
        if (temp.size() > MAX_KEY_LENGTH) {
            throw new DictionaryException("Key is larger than permitted size of " + MAX_KEY_LENGTH + " bytes");
        }
        file.seek(indexPositionToKeyFp(currentNumRecords));
        temp.writeTo(file);
        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        newRecord.write(file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords+1);
    }

    protected void deleteEntryFromIndex(String key, PostHeader header, int currentNumRecords) throws IOException, DictionaryException {
        if (header.indexPosition != currentNumRecords -1) {
            String lastKey = readKeyFromIndex(currentNumRecords-1);
            PostHeader last  = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);
            file.seek(indexPositionToKeyFp(last.indexPosition));
            file.writeUTF(lastKey);
            file.seek(indexPositionToRecordHeaderFp(last.indexPosition));
            last.write(file);
        }
        writeNumRecordsHeader(currentNumRecords-1);
    }

    public synchronized void insertRecord(PostWriter rw) throws DictionaryException, IOException {
        String key = rw.getKey();
        if (recordExists(key)) {
            throw new DictionaryException("Key exists: " + key);
        }
        insureIndexSpace(getNumRecords() + 1);
        PostHeader newRecord = allocateRecord(key, rw.getDataLength());
        writeRecordData(newRecord, rw);
        addEntryToIndex(key, newRecord, getNumRecords());
    }

    public synchronized void updateRecord(PostWriter rw) throws DictionaryException, IOException {
        PostHeader header = keyToRecordHeader(rw.getKey());
        if (rw.getDataLength() > header.dataCapacity) {
            deleteRecord(rw.getKey());
            insertRecord(rw);
        } else {
            writeRecordData(header, rw);
            writeRecordHeaderToIndex(header);
        }
    }

    public synchronized PostReader readRecord(String key) throws DictionaryException, IOException {
        byte[] data = readRecordData(key);
        return new PostReader(key, data);
    }

    protected byte[] readRecordData(String key) throws IOException, DictionaryException {
        return readRecordData(keyToRecordHeader(key));
    }

    protected byte[] readRecordData(PostHeader header) throws IOException {
        byte[] buf = new byte[header.dataCount];
        file.seek(header.dataPointer);
        file.readFully(buf);
        return buf;
    }

    protected void writeRecordData(PostHeader header, PostWriter rw) throws IOException, DictionaryException {
        if (rw.getDataLength() > header.dataCapacity) {
            throw new DictionaryException("Record data does not fit");
        }
        header.dataCount = rw.getDataLength();
        file.seek(header.dataPointer);
        rw.writeTo((DataOutput)file);
    }

    protected void writeRecordData(PostHeader header, byte[] data) throws IOException, DictionaryException {
        if (data.length > header.dataCapacity) {
            throw new DictionaryException("Record data does not fit");
        }
        header.dataCount = data.length;
        file.seek(header.dataPointer);
        file.write(data, 0, data.length);
    }

    public synchronized void deleteRecord(String key) throws DictionaryException, IOException {
        PostHeader delRec = keyToRecordHeader(key);
        int currentNumRecords = getNumRecords();
        if (getFileLength() == delRec.dataPointer + delRec.dataCapacity) {
            // shrink file since this is the last record in the file
            setFileLength(delRec.dataPointer);
        } else {
            PostHeader previous = getRecordAt(delRec.dataPointer -1);
            if (previous != null) {
                // append space of deleted record onto previous record
                previous.dataCapacity += delRec.dataCapacity;
                writeRecordHeaderToIndex(previous);
            } else {
                // target record is first in the file and is deleted by adding its space to
                // the second record.
                PostHeader secondRecord = getRecordAt(delRec.dataPointer + (long)delRec.dataCapacity);
                byte[] data = readRecordData(secondRecord);
                secondRecord.dataPointer = delRec.dataPointer;
                secondRecord.dataCapacity += delRec.dataCapacity;
                writeRecordData(secondRecord, data);
                writeRecordHeaderToIndex(secondRecord);
            }
        }
        deleteEntryFromIndex(key, delRec, currentNumRecords);
    }

    protected void insureIndexSpace(int requiredNumRecords) throws DictionaryException, IOException {
        int currentNumRecords = getNumRecords();
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if (endIndexPtr > getFileLength() && currentNumRecords == 0) {
            setFileLength(endIndexPtr);
            dataStartPtr = endIndexPtr;
            writeDataStartPtrHeader(dataStartPtr);
            return;
        }
        while (endIndexPtr > dataStartPtr) {
            PostHeader first = getRecordAt(dataStartPtr);
            byte[] data = readRecordData(first);
            first.dataPointer = getFileLength();
            first.dataCapacity = data.length;
            setFileLength(first.dataPointer + data.length);
            writeRecordData(first, data);
            writeRecordHeaderToIndex(first);
            dataStartPtr += first.dataCapacity;
            writeDataStartPtrHeader(dataStartPtr);
        }
    }

    public synchronized void close() throws IOException, DictionaryException {
        try {
            file.close();
        } finally {
            file = null;
        }
    }
}