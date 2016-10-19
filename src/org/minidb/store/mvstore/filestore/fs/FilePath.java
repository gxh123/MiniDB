/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore.filestore.fs;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * A path to a file. It similar to the Java 7 <code>java.nio.file.Path</code>,
 * but simpler, and works with older versions of Java. It also implements the
 * relevant methods found in <code>java.nio.file.FileSystem</code> and
 * <code>FileSystems</code>
 */
//FilePath主要提供创建文件，读取文件的功能，根据文件名前缀判断FilePath的具体实现是disk还是nio
//disk和nio主要的区别是open文件的函数，open函数里创建了FileNio或者FileDisk，这两个类决定了使用什么样的方式对文件进行读取
public abstract class FilePath {

    private static final String CLASSPATH_PREFIX = "classpath:";

    private static FilePath defaultProvider;

    private static Map<String, FilePath> providers;

    /**
     * The prefix for temporary files.
     */
    private static String tempRandom;
    private static long tempSequence;

    /**
     * The complete path (which may be absolute or relative, depending on the
     * file system).
     */
    protected String name;

    /**
     * Get the file path object for the given path.
     * Windows-style '\' is replaced with '/'.
     *
     * @param path the path
     * @return the file path object
     */
    public static FilePath get(String path) {
        path = path.replace('\\', '/');
        int index = path.indexOf(':');

        //如E:\\H2\\tmp\\FileStoreTest\\my.txt
        //此时index为1，所以要用FilePathDisk
        //而memFS:E:\\H2\\tmp\\FileStoreTest\\my.txt
        //index是5，所以用FilePathMem
        registerDefaultProviders();
        if (index < 2) {
            // use the default provider if no prefix or
            // only a single character (drive name)
            return defaultProvider.getPath(path);
        }
        String scheme = path.substring(0, index);
        FilePath p = providers.get(scheme);
        if (p == null) {
            // provider not found - use the default
            p = defaultProvider;
        }
        return p.getPath(path);
    }

    private static void registerDefaultProviders() {
        if (providers == null || defaultProvider == null) {
            Map<String, FilePath> map = Collections.synchronizedMap(new HashMap<String, FilePath>());
            //默认是org.h2.store.fs.FilePathDisk, 所以这里不包含它
            //但是少了org.h2.store.fs.FilePathRec、org.h2.mvstore.cache.FilePathCache
            //不过org.h2.store.fs.FilePathRec是通过org.h2.store.fs.FilePath.register(FilePath)这个方法注册
            //见org.h2.store.fs.FilePathRec.register(),
            //在org.h2.engine.ConnectionInfo.ConnectionInfo(String, Properties)调用它了
            for (String c : new String[] {
//                    "FilePathDisk",
//                    "org.h2.store.fs.FilePathMem",
//                    "org.h2.store.fs.FilePathMemLZF",
//                    "org.h2.store.fs.FilePathNioMem",
//                    "org.h2.store.fs.FilePathNioMemLZF",
//                    "org.h2.store.fs.FilePathSplit",
//                    "FilePathNio",
//                    "org.h2.store.fs.FilePathNioMapped",
//                    "org.h2.store.fs.FilePathZip",
//                    "org.h2.store.fs.FilePathRetryOnInterrupt"

                    "org.minidb.store.mvstore.filestore.fs.FilePathDisk",
                    "org.minidb.store.mvstore.filestore.fs.FilePathNio",

            }) {
                try {
                    FilePath p = (FilePath) Class.forName(c).newInstance();
                    map.put(p.getScheme(), p);
                    if (defaultProvider == null) {
                        defaultProvider = p;
                    }
                } catch (Exception e) {
                    // ignore - the files may be excluded in purpose
                }
            }
            providers = map;
        }
    }

    /**
     * Register a file provider.
     *
     * @param provider the file provider
     */
    public static void register(FilePath provider) {
        registerDefaultProviders();
        providers.put(provider.getScheme(), provider);
    }

    /**
     * Unregister a file provider.
     *
     * @param provider the file provider
     */
    public static void unregister(FilePath provider) {
        registerDefaultProviders();
        providers.remove(provider.getScheme());
    }

    /**
     * Get the size of a file in bytes
     *
     * @return the size in bytes
     */
    public long size() {
        return new File(name).length();
    }

    /**
     * Rename a file if this is allowed.
     *
     * @param newName the new fully qualified file name
     * @param atomicReplace whether the move should be atomic, and the target
     *            file should be replaced if it exists and replacing is possible
     */
    public void moveTo(FilePath newName, boolean atomicReplace) {
        File oldFile = new File(name);
        File newFile = new File(newName.name);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            return;
        }
        if (!oldFile.exists()) {
//            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
//                    name + " (not found)",
//                    newName.name);
        }
        // Java 7: use java.nio.file.Files.move(Path source, Path target,
        //     CopyOption... options)
        // with CopyOptions "REPLACE_EXISTING" and "ATOMIC_MOVE".
        if (atomicReplace) {
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
//            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
//                    new String[]{name, newName.name});
        }
        if (newFile.exists()) {
//            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
//                new String[] { name, newName + " (exists)" });
        }
        for (int i = 0; i < 16; i++) {
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
//        throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
//                new String[]{name, newName.name});
    }

    private static void wait(int i) {
        if (i == 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    /**
     * Create a new file.
     *
     * @return true if creating was successful
     */
    public boolean createFile() {
        File file = new File(name);
        for (int i = 0; i < 16; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    /**
     * Checks if a file exists.
     *
     * @return true if it exists
     */
    public boolean exists() {
        if(name.startsWith("nio"))  return new File(name.substring(getScheme().length() + 1)).exists();
        return new File(name).exists();
    }

    /**
     * Delete a file or directory if it exists.
     * Directories may only be deleted if they are empty.
     */
    public void delete() {
        File file = new File(name);
        for (int i = 0; i < 16; i++) {
            boolean ok = file.delete();
            if (ok || !file.exists()) {
                return;
            }
            wait(i);
        }
//        throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, name);
    }

    /**
     * List the files and directories in the given directory.
     *
     * @return the list of fully qualified file names
     */
    //文件和目录名都会列出来
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = new ArrayList();
        File f = new File(name);
        try {
            String[] files = f.list();
            if (files != null) {
                String base = f.getCanonicalPath();
                if (!base.endsWith("/")) {
                    base += "/";
                }
                for (int i = 0, len = files.length; i < len; i++) {
                    list.add(getPath(base + files[i]));
                }
            }
            return list;
        } catch (IOException e) {
            return null;
//            throw DbException.convertIOException(e, name);
        }
    }

    /**
     * Normalize a file name.
     *
     * @return the normalized file name
     */
    public FilePath toRealPath() {
        try {
            String fileName = new File(name).getCanonicalPath();
            return getPath(fileName);
        } catch (IOException e) {
            return null;
//            throw DbException.convertIOException(e, name);
        }
    }


    /**
     * Get the parent directory of a file or directory.
     *
     * @return the parent directory name
     */
    public FilePath getParent() {
        String p = new File(name).getParent();
        return p == null ? null : getPath(p);
    }

    /**
     * Check if it is a file or a directory.
     *
     * @return true if it is a directory
     */
    public boolean isDirectory() {
        return new File(name).isDirectory();
    }

    /**
     * Check if the file name includes a path.
     *
     * @return if the file name is absolute
     */
    public boolean isAbsolute() {
        return new File(name).isAbsolute();
    }

    /**
     * Get the last modified date of a file
     *
     * @return the last modified date
     */
    public long lastModified() {
        return new File(name).lastModified();
    }

    /**
     * Check if the file is writable.
     *
     * @return if the file is writable
     */
    public boolean canWrite() {
        return canWriteInternal(new File(name));
    }

    /**
     * Create a directory (all required parent directories already exist).
     */
    public void createDirectory() {
        File dir = new File(name);
        for (int i = 0; i < 16; i++) {
            if (dir.exists()) {
                if (dir.isDirectory()) {
                    return;
                }
//                throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
//                        name + " (a file with this name already exists)");
            } else if (dir.mkdir()) {
                return;
            }
            wait(i);
        }
//        throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name);
    }

    /**
     * Get the file or directory name (the last element of the path).
     *
     * @return the last element of the path
     */
    public String getName() {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    /**
     * Create an output stream to write into the file.
     *
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public OutputStream newOutputStream(boolean append) throws IOException {
        try {
            File file = new File(name);
            File parent = file.getParentFile();
            if (parent != null) {
                FileUtils.createDirectories(parent.getAbsolutePath());
            }
            FileOutputStream out = new FileOutputStream(name, append);
            return out;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new FileOutputStream(name);
        }
    }

    /**
     * Call the garbage collection and run finalization. This close all files
     * that were not closed, and are no longer referenced.
     */
    static void freeMemoryAndFinalize() {
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    /**
     * Open a random access file object.
     *
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public abstract FileChannel open(String mode) throws IOException;

    /**
     * Create an input stream to read from the file.
     *
     * @return the input stream
     */
    public InputStream newInputStream() throws IOException {
        int index = name.indexOf(':');
        if (index > 1 && index < 20) {
            //如"classpath:my/test/store/fs/FileUtilsTest.class"
            // if the ':' is in position 1, a windows file access is assumed:
            // C:.. or D:, and if the ':' is not at the beginning, assume its a
            // file name with a colon
            if (name.startsWith(CLASSPATH_PREFIX)) {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                InputStream in = getClass().getResourceAsStream(fileName);
                if (in == null) {
                    in = Thread.currentThread().getContextClassLoader().
                            getResourceAsStream(fileName);
                }
                if (in == null) {
                    throw new FileNotFoundException("resource " + fileName);
                }
                return in;
            }
            // otherwise an URL is assumed
            URL url = new URL(name);
            InputStream in = url.openStream();
            return in;
        }
        FileInputStream in = new FileInputStream(name);
        return in;
    }

    /**
     * Disable the ability to write.
     *
     * @return true if the call was successful
     */
    public boolean setReadOnly() {
        File f = new File(name);
        return f.setReadOnly();
    }

    /**
     * Create a new temporary file.
     *
     * @param suffix the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *            machine exists
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
                                   boolean inTempDir) throws IOException {
        while (true) {
            FilePath p = getPath(name + getNextTempFileNamePart(false) + suffix);
            if (p.exists() || !p.createFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            p.open("rw").close();
            return p;
        }
    }

    /**
     * Get the next temporary file name part (the part in the middle).
     *
     * @param newRandom if the random part of the filename should change
     * @return the file name part
     */
    protected static synchronized String getNextTempFileNamePart(
            boolean newRandom) {
        if (newRandom || tempRandom == null) {
            tempRandom = new Random().nextInt(Integer.MAX_VALUE) + ".";
        }
        return tempRandom + tempSequence++;
    }

    private static boolean canWriteInternal(File file) {
        try {
            if (!file.canWrite()) {
                return false;
            }
        } catch (Exception e) {
            // workaround for GAE which throws a
            // java.security.AccessControlException
            return false;
        }
        // File.canWrite() does not respect windows user permissions,
        // so we must try to open it using the mode "rw".
        // See also http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Get the string representation. The returned string can be used to
     * construct a new object.
     *
     * @return the path as a string
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Get the scheme (prefix) for this file provider.
     * This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getScheme</code>.
     *
     * @return the scheme
     */
    public abstract String getScheme();

    /**
     * Convert a file to a path. This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getPath</code>, but may
     * return an object even if the scheme doesn't match in case of the the
     * default file provider.
     *
     * @param path the path
     * @return the file path object
     */
    public abstract FilePath getPath(String path);

    /**
     * Get the unwrapped file name (without wrapper prefixes if wrapping /
     * delegating file systems are used).
     *
     * @return the unwrapped path
     */
    public FilePath unwrap() {
        return this;
    }

}
