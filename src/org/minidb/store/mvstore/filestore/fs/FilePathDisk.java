/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore.filestore.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;

/**
 * This file system stores files on disk.
 * This is the most common file system.
 */
//
public class FilePathDisk extends FilePath {

    private static final String CLASSPATH_PREFIX = "classpath:";

    @Override
    public FilePathDisk getPath(String path) {
        FilePathDisk p = new FilePathDisk();
        //name是父类FilePath的字段
        p.name = translateFileName(path);
        return p;
    }

    /**
     * Translate the file name to the native format. This will replace '\' with
     * '/' and expand the home directory ('~').
     *
     * @param fileName the file name
     * @return the native file name
     */
    //去掉file这个模式前缀，并且替换\到/，替换~到USER_HOME
    protected static String translateFileName(String fileName) {
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("file:")) {
            fileName = fileName.substring("file:".length());
        }
        return expandUserHomeDirectory(fileName);
    }

    /**
     * Expand '~' to the user home directory. It is only be expanded if the '~'
     * stands alone, or is followed by '/' or '\'.
     *
     * @param fileName the file name
     * @return the native file name
     */
    //例如System.setProperty("user.home", "E:/H2/tmp");
	//fileName = "file:~/FileStoreTest/my.txt";
    //则返回的是E:/H2/tmp/FileStoreTest/my.txt
    public static String expandUserHomeDirectory(String fileName) {
    	//要么是单个~，要么是以~/开头
        if (fileName.startsWith("~") && (fileName.length() == 1 || fileName.startsWith("~/"))) {
            String userDir = ""; //默认是C:\Users\Administrator
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        FileDisk f;
        try {
            f = new FileDisk(name, mode);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileDisk(name, mode);
            } catch (IOException e2) {
                throw e;
            }
        }
        return f;
    }

    @Override
    public String getScheme() {
        return "file";
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
                                   boolean inTempDir) throws IOException {
        String fileName = name + ".";
        String prefix = new File(fileName).getName();
        File dir;
        if (inTempDir) {
            dir = new File(System.getProperty("java.io.tmpdir", "."));
        } else {
            dir = new File(fileName).getAbsoluteFile().getParentFile();
        }
        FileUtils.createDirectories(dir.getAbsolutePath());
        while (true) {
            File f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
            if (f.exists() || !f.createNewFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            if (deleteOnExit) {
                try {
                    f.deleteOnExit();
                } catch (Throwable e) {
                    // sometimes this throws a NullPointerException
                    // at java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                    // we can ignore it
                }
            }
            return get(f.getCanonicalPath());
        }
    }

}

/**
 * Uses java.io.RandomAccessFile to access a file.
 */
//使用RandomAccessFile对文件进行读取
class FileDisk extends FileBase {

    private final RandomAccessFile file;
    private final String name;
    private final boolean readOnly;

    FileDisk(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
        this.name = fileName;
        this.readOnly = mode.equals("r");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        String m = "sync";
        if ("".equals(m)) {
            // do nothing
        } else if ("sync".equals(m)) {
            file.getFD().sync();
        } else if ("force".equals(m)) {
            file.getChannel().force(true);
        } else if ("forceFalse".equals(m)) {
            file.getChannel().force(false);
        } else {
            file.getFD().sync();
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (readOnly) {
            throw new NonWritableChannelException();
        }
        if (newLength < file.length()) {
            file.setLength(newLength);
        }
        return this;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

    @Override
    public void implCloseChannel() throws IOException {
        file.close();
    }

    @Override
    public long position() throws IOException {
        return file.getFilePointer();
    }

    @Override
    public long size() throws IOException {
        return file.length();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = file.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
        }
        return len;
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        file.seek(pos);
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        file.write(src.array(), src.arrayOffset() + src.position(), len);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public String toString() {
        return name;
    }

}
