/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore;

//import org.h2.mvstore.cache.FilePathCache;
import org.minidb.store.mvstore.filestore.fs.FilePath;
import org.minidb.store.mvstore.filestore.fs.FilePathDisk;
import org.minidb.store.mvstore.filestore.fs.FilePathNio;
import org.minidb.store.mvstore.filestore.fs.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * The default storage mechanism of the MVStore. This implementation persists
 * data to a file. The file store is responsible to persist data and for free
 * space management.
 */
public class FileStore {

    protected String fileName;
    protected long fileSize;
    protected FileChannel file;
    protected FileLock fileLock;

    @Override
    public String toString() {
        return fileName;
    }

    /**
     * Read from the file.
     *
     * @param pos the write position
     * @param len the number of bytes to read
     * @return the byte buffer
     */
    public ByteBuffer readFully(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        FileUtils.readFully(file, pos, dst);
        return dst;
    }

    /**
     * Write to the file.
     *
     * @param pos the write position
     * @param src the source buffer
     */
    public void writeFully(long pos, ByteBuffer src) {
        int len = src.remaining();
        fileSize = Math.max(fileSize, pos + len);
        FileUtils.writeFully(file, pos, src);
    }

    /**
     * Try to open the file.
     *
     * @param fileName the file name
     */
    public void open(String fileName) {
        if (file != null) {
            return;
        }
        if (fileName != null) {
            FilePath p = FilePath.get(fileName);
            // if no explicit scheme was specified, NIO is used
            if (p instanceof FilePathDisk &&
                    !fileName.startsWith(p.getScheme() + ":")) {
                // ensure the NIO file system is registered
                FilePathNio.class.getName();
                fileName = "nio:" + fileName;
            }
        }
        this.fileName = fileName;
        FilePath f = FilePath.get(fileName);
        FilePath parent = f.getParent();
        if (parent != null && !parent.exists()) {
            throw new RuntimeException("Directory does not exist");
        }
        try {
            file = f.open("rw");
            try {
                fileLock = file.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new RuntimeException("The file is locked");
            }
            if (fileLock == null) {
                throw new RuntimeException("The file is locked");
            }
            fileSize = file.size();
        } catch (IOException e) {
            throw new RuntimeException("Could not open file");
        }
    }

    /**
     * Close this store.
     */
    public void close() {
        try {
            if (fileLock != null) {
                fileLock.release();
                fileLock = null;
            }
            file.close();
        } catch (Exception e) {
            throw new RuntimeException("Closing failed for file");
        } finally {
            file = null;
        }
    }

    /**
     * Flush all changes.
     */
    public void sync() {
        try {
            file.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Could not sync file ");
        }
    }

    public long size() {
        return fileSize;
    }

    public FileChannel getFile() {
        return file;
    }

    public int getDefaultRetentionTime() {
        return 45000;
    }

    public String getFileName() {
        return fileName;
    }

}
