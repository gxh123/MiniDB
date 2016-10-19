package org.minidb.store.mvstore;

import org.minidb.store.mvstore.type.StringDataType;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by gxh on 2016/6/29.
 */
public class MVStore {

    static final int BLOCK_SIZE = 4 * 1024;

    long currentVersion;
    private volatile long currentStoreVersion = -1;
    int lastMapId;
    protected FileStore fileStore;
    private Chunk lastChunk;
    protected int versionsToKeep = 5;
    private int retentionTime;
    private int autoCommitDelay;
    private long lastStoredVersion;
    private volatile boolean metaChanged;


    private WriteBuffer writeBuffer;
    private HashMap<String, Object> storeHeader = new HashMap();

    //maps只用来存map，不支持回滚，回滚要通过meta进行
    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps =
            new ConcurrentHashMap<Integer, MVMap<?, ?>>();

    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<Integer, Chunk>();

    //meta作用很大，首先本身是MVMap，支持回滚，其次meta存了map，chunk，等的名字，配置。
    //meta不能使用普通的HashMap，HashMap没有回滚功能
    private MVMap<String, String> meta;

    public MVStore(HashMap<String, Object> config) {
        meta = new MVMap<String, String>(StringDataType.INSTANCE,
                StringDataType.INSTANCE);
        HashMap<String, Object> c = new HashMap();
        c.put("id", 0);
        c.put("createVersion", currentVersion);
        meta.init(this, c);

        String fileName = (String) config.get("fileName");
        fileStore = (FileStore)config.get("fileStore");
        if (fileName == null && fileStore == null) {
            return;
        }
        if(fileStore == null){
            fileStore = new FileStore();
            fileStore.open(fileName);
        }
        if(fileStore.size() == 0){    //里面什么都没有
            writeStoreHeader();
        } else {
            readStoreHeader();
        }

    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder();
        if (lastChunk != null) {
            storeHeader.put("block", lastChunk.block);
            storeHeader.put("chunk", lastChunk.id);
            storeHeader.put("version", lastChunk.version);
        }
        storeHeader.put("name", "miniDb");
        storeHeader.put("blockSize", BLOCK_SIZE);
        storeHeader.put("format", 1);
        storeHeader.put("created", 450);
        DataUtil.appendMap(buff, storeHeader);       //将storeHeader这个HashMap里的键值对转换为字符串放入buff中
        byte[] bytes = buff.toString().getBytes(Charset.forName("ISO-8859-1"));   //转换为byte数组
        int checksum = DataUtil.getFletcher32(bytes, bytes.length);               //用byte数组计算校验和
        DataUtil.appendMap(buff, "fletcher", checksum);                           //buff后面加入校验和
        buff.append("\n");
        bytes = buff.toString().getBytes(Charset.forName("ISO-8859-1"));          //整体转换为byte数组
        ByteBuffer header = ByteBuffer.allocate(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);                                                         //写入
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            throw e;
        }
    }

    public long commit() {
        if( fileStore != null){
            if (!hasUnsavedChanges()) {
//                System.out.println("没有unsaved");
                return currentVersion;
            }
            return storeNowTry();
        }
        long v = ++currentVersion;
        setWriteVersion(v);
        return v;
    }

    public boolean hasUnsavedChanges() {
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            long v = m.getVersion();
            if (v >= 0 && v > lastStoredVersion) {
                return true;
            }
        }
        return false;
    }

    private synchronized void freeUnusedChunks() {

    }

    private long storeNowTry() {
        freeUnusedChunks();   //还没写，删掉老的chunk

        long storeVersion = currentVersion;
        long version = ++currentVersion;
        setWriteVersion(version);

        ArrayList<MVMap<?, ?>> list = new ArrayList(maps.values());
        ArrayList<MVMap<?, ?>> changed = new ArrayList();
        for (MVMap<?, ?> m : list) {          //通过版本的判断找到所有发生改变的map
            m.setWriteVersion(version);
            long v = m.getVersion();
            if (m.getCreateVersion() > storeVersion) {
                // the map was created after storing started
                continue;
            }
            if (v >= 0 && v >= lastStoredVersion) {
                MVMap<?, ?> r = m.openVersion(storeVersion);
                if (r.getRoot().getPos() == 0) {
                    changed.add(r);
                }
            }
        }

        int newChunkId;
        if(lastChunk == null){
            newChunkId = 1;
        } else {
            newChunkId = lastChunk.id + 1;    //简化操作，就做加1处理
        }
        Chunk c = new Chunk(newChunkId);      //给当前要存的这个版本创建一个Chunk
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.version = version;
        c.metaRootPos = Long.MAX_VALUE;
        c.mapId = lastMapId;
        c.next = Long.MAX_VALUE;

        WriteBuffer buff = getWriteBuffer();
        c.writeChunkHeader(buff, 0);          //给header在buff先占个位置，后面更新
        int headerLength = buff.position();
        for (MVMap<?, ?> m : changed) {       //将所有改变的map写入buff里
            Page p = m.getRoot();
            String key = MVMap.getMapRootKey(m.getId());
            if (p.getTotalCount() == 0) {
                meta.put(key, "0");
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }
        meta.setWriteVersion(version);

        Page metaRoot = meta.getRoot();
        metaRoot.writeUnsavedRecursive(c, buff);  //将meta写入buff里

        int chunkLength = buff.position();
        // add the store header and round to the next block
        int length = roundUpInt(chunkLength + Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        long filePos = getFileLengthInUse();     //获取在文件中写入的起始位置
        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        c.metaRootPos = metaRoot.getPos();
        c.next =  c.block + c.len;   //先这么简单的实现
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);       //header
//        revertTemp(storeVersion);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());                 //footer
        buff.position(0);
        write(filePos, buff.getBuffer());             //在文件中写入buff的内容
//        releaseWriteBuffer(buff);

        lastChunk = c;
        meta.put(Chunk.getMetaKey(c.id), c.asString());   //metaChanged 不使用，直接更新没关系
        chunks.put(c.id, c);
        revertTemp(version);
        metaChanged = false;
        lastStoredVersion = storeVersion;
        return version;
    }

    private void revertTemp(long storeVersion) {
        for (MVMap<?, ?> m : maps.values()) {
            m.removeUnusedOldVersions();
        }
    }

    private long getFileLengthInUse() {
        long size = BLOCK_SIZE;
        for (Chunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                long x = (c.block + c.len) * BLOCK_SIZE;
                size = Math.max(size, x);
            }
        }
        return size;
    }


    public static int roundUpInt(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }


    //更新所有的map（meta也是map）的WriteVersion
    private void setWriteVersion(long version) {
        for (MVMap<?, ?> map : maps.values()) {
            map.setWriteVersion(version);
        }
        MVMap<String, String> m = meta;
        m.setWriteVersion(version);
    }

//    public <K, V> MVMap<K, V> openMap(String name) {
//        MVMap.Builder<String, String> builder = new MVMap.Builder<String, String>();
//        builder.keyType(new StringDataType());
//        builder.valueType(new StringDataType());
//        return openMap(name, new MVMap.Builder<K, V>());
//    }

    public <K, V> MVMap<K, V> openMap(String name) {
        return openMap(name, new MVMap.Builder<K, V>());
    }

    //openMap主要步骤：在meta根据map名字找到mapID，根据mapID在maps里找到map
    //如果meta里都没有，就要新建一个map，然后将map的信息放入meta，然后在maps里放入这个map
    //如果meta里有，maps里没有（产生的原因主要是meta是MVMap可以rollback），这个时候就要根据meta存的这个map的信息来新建一个map，然后在maps里放入这个map
    //meta存储map的形式比较奇怪（并没有想出更好的方法），meta的键和值都是字符串，一个map的信息存了两组键值对，一组是通过名字获取id，另一组是通过id获取配置
    // "name." + map名字 作为键，mapID作为值; "map." + mapID 作为键，一组配置作为值
    public synchronized <M extends MVMap<K, V>, K, V> M openMap(
            String name, MVMap.MapBuilder<M, K, V> builder) {
        String x = meta.get("name." + name);      //通过名字获取id
        int id;
        long root;
        HashMap<String, Object> c;
        M map;
        if (x != null) {
            id = (int) Long.parseLong(x, 16);
            M old = (M) maps.get(id);
            if (old != null) {
                return old;
            }
            map = builder.create();
            String config = meta.get(MVMap.getMapKey(id));    //通过id获取配置
            c = new HashMap();
            c.putAll(DataUtil.parseMap(config));
            c.put("id", id);
            map.init(this, c);
            root = getRootPos(meta, id);
        } else {
            c = new HashMap();
            id = ++lastMapId;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create();
            map.init(this, c);
            markMetaChanged();
            x = Integer.toHexString(id);
            meta.put(MVMap.getMapKey(id), map.asString(name));   //放入mapID，map配置键值对
            meta.put("name." + name, x);                         //放入名字，mapID键值对
            root = 0;
        }
        map.setRootPos(root, -1);
        maps.put(id, map);              //在maps里放入这个map
        return map;
    }

    public void rollback() {
        rollbackTo(currentVersion);
    }

    //整个MVStore rollback到某一个版本主要是：所有的map（包括meta） rollback，删掉那些回滚版本之后的版本才创建的map，修改currentVersion为回滚的那个版本
    public synchronized void rollbackTo(long version) {
        for (MVMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }

        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // find out which chunks to remove,
        // and which is the newest chunk to keep
        // (the chunk list can have gaps)
        ArrayList<Integer> remove = new ArrayList<Integer>();
        Chunk keep = null;
        for (Chunk c : chunks.values()) {
            if (c.version > version) {
                remove.add(c.id);      //删掉所要版本之后产生的Chunk
            } else if (keep == null || keep.id < c.id) {
                keep = c;              //找到所要的版本里最新的Chunk
            }
        }
        if (remove.size() > 0) {
            loadFromFile = true;
            for (int id : remove) {
                Chunk c = chunks.remove(id);            //从chunks里去掉
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                // overwrite the chunk,
                // so it is not be used later on
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                // buff.clear() does not set the data
                Arrays.fill(buff.getBuffer().array(), (byte) 0);   //写入0
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
            }
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader();
        }

        for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                maps.remove(id);    //回滚到的版本有的map还没有创建，删掉这些map
            } else {
                if (loadFromFile) {    //采用磁盘保存的方式的话，要获取历史版本的map，只有从文件中读取
                    m.setRootPos(getRootPos(meta, id), -1);
                }
            }
        }
        if (lastChunk != null) {
            for (Chunk c : chunks.values()) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
        }
        currentVersion = version;
        setWriteVersion(version);
    }

    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtil.parseHexLong(root);
    }

    Page readPage(MVMap<?, ?> map, long pos) {
        if (pos == 0) {
            throw new RuntimeException("readPage ERROR");
        }
        Chunk c = getChunk(pos);
        long filePos = c.block * BLOCK_SIZE;
        filePos += DataUtil.getPageOffset(pos);
        long maxPos = (c.block + c.len) * BLOCK_SIZE;
        Page p = Page.read(fileStore, pos, map, filePos, maxPos);
        return p;
    }

    private Chunk getChunk(long pos) {
        Chunk c = getChunkIfFound(pos);
        if (c == null) {
            throw new RuntimeException("Chunk not found");
        }
        return c;
    }

    private Chunk getChunkIfFound(long pos) {
        int chunkId = DataUtil.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                return null;
            }
            c = Chunk.fromString(s);
            chunks.put(c.id, c);
        }
        return c;
    }

    private synchronized void readStoreHeader() {
        Chunk newest = null;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 1 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i += BLOCK_SIZE) {   //这里只存了一遍
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                String s = new String(buff, 0, BLOCK_SIZE,
                        DataUtil.LATIN).trim();
                HashMap<String, String> m = DataUtil.parseMap(s);
                int check = DataUtil.readHexInt(m, "fletcher", 0);
                m.remove("fletcher");
                s = s.substring(0, s.lastIndexOf("fletcher") - 1);
                byte[] bytes = s.getBytes(DataUtil.LATIN);
                int checksum = DataUtil.getFletcher32(bytes,
                        bytes.length);
                if (check != checksum) {
                    continue;
                }
                long version = DataUtil.readHexLong(m, "version", 0);
                if (newest == null || version > newest.version) {
                    storeHeader.putAll(m);
                    int chunkId = DataUtil.readHexInt(m, "chunk", 0);
                    long block = DataUtil.readHexLong(m, "block", 0);
                    Chunk test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == chunkId) {
                        newest = test;
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }
        lastStoredVersion = -1;
        chunks.clear();
        Chunk test = readChunkFooter(fileStore.size());
        if (test != null) {
            test = readChunkHeaderAndFooter(test.block);
            if (test != null) {
                if (newest == null || test.version > newest.version) {
                    newest = test;
                }
            }
        }
        if (newest == null) {
            // no chunk
            return;
        }
        // read the chunk header and footer,
        // and follow the chain of next chunks
        while (true) {
            if (newest.next == 0 ||
                    newest.next >= fileStore.size() / BLOCK_SIZE) {
                // no (valid) next
                break;
            }
            test = readChunkHeaderAndFooter(newest.next);
            if (test == null || test.id <= newest.id) {
                break;
            }
            newest = test;
        }
        setLastChunk(newest);
        loadChunkMeta();
    }

    private Chunk readChunkHeaderAndFooter(long block) {
        Chunk header;
        try {
            header = readChunkHeader(block);
        } catch (Exception e) {
            // invalid chunk header: ignore, but stop
            return null;
        }
        if (header == null) {
            return null;
        }
        Chunk footer = readChunkFooter((block + header.len) * BLOCK_SIZE);
        if (footer == null || footer.id != header.id) {
            return null;
        }
        return header;
    }

    private Chunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, Chunk.MAX_HEADER_LENGTH);
        return Chunk.readChunkHeader(buff, p);
    }

    /**
     * Try to read a chunk footer.
     *
     * @param end the end of the chunk
     * @return the chunk, or null if not successful
     */
    private Chunk readChunkFooter(long end) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            ByteBuffer lastBlock = fileStore.readFully(
                    end - Chunk.FOOTER_LENGTH, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            String s = new String(buff, DataUtil.LATIN).trim();
            HashMap<String, String> m = DataUtil.parseMap(s);
            int check = DataUtil.readHexInt(m, "fletcher", 0);
            m.remove("fletcher");
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(DataUtil.LATIN);
            int checksum = DataUtil.getFletcher32(bytes, bytes.length);
            if (check == checksum) {
                int chunk = DataUtil.readHexInt(m, "chunk", 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtil.readHexLong(m, "version", 0);
                c.block = DataUtil.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void loadChunkMeta() {
        // load the chunk metadata: we can load in any order,
        // because loading chunk metadata might recursively load another chunk
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            Chunk c = Chunk.fromString(s);
            if (!chunks.containsKey(c.id)) {
                chunks.put(c.id, c);
            }
        }
    }

    private void setLastChunk(Chunk last) {
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId = 0;
            currentVersion = 0;
            meta.setRootPos(0, -1);
        } else {
            lastMapId = last.mapId;
            currentVersion = last.version;
            chunks.put(last.id, last);
            meta.setRootPos(last.metaRootPos, -1);
        }
        setWriteVersion(currentVersion);
    }

    void removePage(MVMap<?, ?> map, long pos, int memory) {
    }

    public void close() {
        FileStore f = fileStore;
        if (f != null ) {
//            storeNowTry();
        }
        closeStore(true);
    }

    private void closeStore(boolean shrinkIfPossible) {
        if (fileStore == null) {
            return;
        }
        synchronized (this) {
            meta = null;
            chunks.clear();
            maps.clear();
            fileStore.close();
            fileStore = null;
        }
    }

    public static MVStore open(String fileName) {
        HashMap<String, Object> config = new HashMap();
        config.put("fileName", fileName);
        return new MVStore(config);
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    public synchronized String getMapName(int id) {
        String m = meta.get(MVMap.getMapKey(id));
        return m == null ? null : DataUtil.parseMap(m).get("name");
    }

    public boolean hasMap(String name) {
        return meta.containsKey("name." + name);
    }

    public Map<String, Object> getStoreHeader() {
        return storeHeader;
    }

    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
     * <p>
     * This setting is not persisted.
     *
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */
    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    public synchronized void removeMap(MVMap<?, ?> map) {
        map.clear();
        int id = map.getId();
        String name = getMapName(id);
        meta.remove(MVMap.getMapKey(id));
        meta.remove("name." + name);
        meta.remove(MVMap.getMapRootKey(id));
        maps.remove(id);
    }

    public void setAutoCommitDelay(int millis) {
        autoCommitDelay = millis;
    }

    public MVMap<String, String> getMetaMap() {
        return meta;
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        c = readChunkHeader(c.block);
        MVMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        Chunk newest = null;
        for (Chunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    <T extends MVMap<?, ?>> T openMapVersion(long version, int mapId,
                                             MVMap<?, ?> template) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        long rootPos = getRootPos(oldMeta, mapId);
        MVMap<?, ?> m = template.openReadOnly();
        m.setRootPos(rootPos, version);
        return (T) m;
    }

    /**
     * A builder for an MVStore.
     */
    public static class Builder {

        private final HashMap<String, Object> config = new HashMap();

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * Disable auto-commit, by setting the auto-commit delay and auto-commit
         * buffer size to 0.
         *
         * @return this
         */
        public Builder autoCommitDisabled() {
            // we have a separate config option so that
            // no thread is started if the write delay is 0
            // (if we only had a setter in the MVStore,
            // the thread would need to be started in any case)
            set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        public Builder autoCommitBufferSize(int kb) {
            return set("autoCommitBufferSize", kb);
        }

        public Builder fileName(String fileName) {
            return set("fileName", fileName);
        }

        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        public MVStore open() {
            return new MVStore(config);
        }

        @Override
        public String toString() {
            return DataUtil.appendMap(new StringBuilder(), config).toString();
        }

        public static Builder fromString(String s) {
            HashMap<String, String> config = DataUtil.parseMap(s);
            Builder builder = new Builder();
            builder.config.putAll(config);
            return builder;
        }

    }
}
