package org.minidb.store.mvstore;

import org.minidb.store.mvstore.type.DataType;

import java.nio.ByteBuffer;

/*
每一个page都是b-树的节点，可能是内部节点，可能是叶子节点。
page主要的属性是：map（属于哪个map），version（版本），memory（整个page所占的内存）
                keys（键，数组存储），values（值，数组存储），children（子节点引用，数组存储）
*/
public class Page {

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private final MVMap<?, ?> map;
    private long version;
    private long totalCount;
    private int cachedCompare;
    private int memory;
    private Object[] keys;
    private Object[] values;
    private PageReference[] children;
    private long pos;       //由chunkid，在chunk里的offset，length，type（叶子还是内部节点）共同组成

    Page(MVMap<?, ?> map, long version) {
        this.map = map;
        this.version = version;
    }

    static Page createEmpty(MVMap<?, ?> map, long version) {
        return create(map, version,
                EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY,
                null, 0, 128);
    }

    //根据指定参数与内容创建page
    public static Page create(MVMap<?, ?> map, long version,
                              Object[] keys, Object[] values, PageReference[] children,
                              long totalCount, int memory) {
        Page p = new Page(map, version);
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();     //不知道所占内存，所以参数为0，需要重新计算memory
        } else {
            p.addMemory(memory);       //知道所占内存，直接赋值
        }
        return p;
    }

    //在一个叶子page内部，往keys数组指定位置（index）插入key，values数组指定位置插入value，原来的index及以后的key，value全部后移一个。
    //在原来的memory基础上增加新加的key和value所占的内存大小，totalCount加1
    public void insertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtil.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtil.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        addMemory(map.getKeyType().getMemory(key) +
                map.getValueType().getMemory(value));
    }

    //在一个内部节点page内部，往keys数组指定位置（index）插入key，children数组指定位置插入childPage引用，原来的index及以后的key，引用全部后移一个。
    //在原来的memory基础上增加新加的key和引用（大小固定为16字节）所占的内存大小，totalCount加新加的page的count
    public void insertNode(int index, Object key, Page childPage) {

        Object[] newKeys = new Object[keys.length + 1];
        DataUtil.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtil.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(
                childPage, childPage.getPos(),childPage.totalCount);
        children = newChildren;

        totalCount += childPage.totalCount;
        addMemory(map.getKeyType().getMemory(key) + 16);
    }

    //当page所用的内存（memory属性）大于一定值，默认4k时，会对这个page进行split，叶子节点和内部节点split程序不一样
    Page split(int at) {
        return isLeaf() ? splitLeaf(at) : splitNode(at);
    }

    //splitLeaf是将原来的page分成两个page，左边的page使用原来的page对象，将keys，values，totalCount属性换成at左边的那部分
    //右边的page为新new出来的page对象，将keys，values，totalCount属性赋值为at右边的那部分
    //返回右边的page,新的page
    private Page splitLeaf(int at) { //小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;                 //原来的keys变为左边的那部分key
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;            //原来的values变为左边的那部分value
        totalCount = a;
        Page newPage = create(map, version,
                bKeys, bValues,
                null,
                bKeys.length, 0);
        return newPage;
    }

    private Page splitNode(int at) {
        int a = at, b = keys.length - a;

        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        long t = 0;
        for (PageReference x : aChildren) {
            t += x.count;
        }
        totalCount = t;
        t = 0;
        for (PageReference x : bChildren) {
            t += x.count;
        }
        Page newPage = create(map, version,
                bKeys, null,
                bChildren,
                t, 0);
        return newPage;
    }

    public void remove(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        Object old = keys[keyIndex];
        addMemory(-map.getKeyType().getMemory(old));
        Object[] newKeys = new Object[keyLength - 1];
        DataUtil.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;

        if (values != null) {
            old = values[index];
            addMemory(-map.getValueType().getMemory(old));
            Object[] newValues = new Object[keyLength - 1];
            DataUtil.copyExcept(values, newValues, keyLength, index);
            values = newValues;
            totalCount--;
        }
        if (children != null) {
            addMemory(-16);
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtil.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }
    }
    /*---------------------------------------------------------------------------------------------------------------*/
    //在index位置设置新的key，修改可能引起的memory变化
    public void setKey(int index, Object key) {
        // this is slightly slower:
        // keys = Arrays.copyOf(keys, keys.length);
        keys = keys.clone();
        Object old = keys[index];            //获取原来的key，进而获取所占存储空间，减掉以后再加上新的key的
        DataType keyType = map.getKeyType();
        int mem = keyType.getMemory(key);
        if (old != null) {
            mem -= keyType.getMemory(old);    //注意，原来的key跟后来的key可能大小不一样！！
        }
        addMemory(mem);
        keys[index] = key;
    }

    //在index位置设置新的value，修改可能引起的memory变化
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length); //只copy引用
        values = values.clone();
        DataType valueType = map.getValueType();    //获取原来的value，进而获取所占存储空间，减掉以后再加上新的value的
        addMemory(valueType.getMemory(value) -
                valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    //Create a copy of this page.
    public Page copy(long version) {
        Page newPage = create(map, version,
                keys, values,
                children, totalCount,
                getMemory());
        // mark the old as deleted
        newPage.cachedCompare = cachedCompare;
        return newPage;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        Object[] k = keys;
        while (low <= high) {
            int compare = map.compare(key, k[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                cachedCompare = x + 1;
                return x;
            }
            x = (low + high) >>> 1;
        }
        cachedCompare = low;
        return -(low + 1);

        // regular binary search (without caching)
        // int low = 0, high = keys.length - 1;
        // while (low <= high) {
        //     int x = (low + high) >>> 1;
        //     int compare = map.compare(key, keys[x]);
        //     if (compare > 0) {
        //         low = x + 1;
        //     } else if (compare < 0) {
        //         high = x - 1;
        //     } else {
        //         return x;
        //     }
        // }
        // return -(low + 1);
    }

    //计算page所占用的memory：page对象（128）+ keys + （如果是叶子节点，则children为null，只要加上所有的value）+ （如果是内部节点，则values为null，只要加上所有的子page引用）
    private void recalculateMemory() {
        int mem = 128;                                  //用来表示page对象的128B
        DataType keyType = map.getKeyType();
        for (int i = 0; i < keys.length; i++) {
            mem += keyType.getMemory(keys[i]);          //加上所有的key
        }
        if (this.isLeaf()) {
            DataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);  //如果是叶子节点，则加上所有的value
            }
        } else {
            mem += this.getRawChildPageCount() * 16;    //如果是内部节点，则加上所有的指向子节点的引用
        }
        addMemory(mem - memory);
    }

    //设置index位置的子page引用
    public void setChild(int index, Page c) {
        if (c == null) {                        //如果新的page为null，则构建一个新的PageReference，它的page属性为null,同时减掉原来子page的条目数目
            long oldCount = children[index].count;
            children = children.clone();  //基本上性能等于 Arrays.copyOf
            PageReference ref = new PageReference(null,0, 0);
            children[index] = ref;
            totalCount -= oldCount;
        } else if (c != children[index].page ){    //如果新的page与原来的page不一样，则new PageReference，替换掉原来的
            long oldCount = children[index].count;
            children = children.clone();
            PageReference ref = new PageReference(c, c.pos, c.totalCount);
            children[index] = ref;
            totalCount += c.totalCount - oldCount;
        }
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param c the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(Chunk c, WriteBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(c, buff);
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                Page p = children[i].page;
                if (p != null) {
                    p.writeUnsavedRecursive(c, buff);
                    children[i] = new PageReference(p, p.getPos(), p.totalCount);    //这里看上去只更新了children的pos，要这么重新new一个吗？
                }
            }
            int old = buff.position();  //记录最终位置
            buff.position(patch);       //跳到要回填的位置
            writeChildren(buff); //writeChildren(chunk, buff)中的children pos可能为0，在这回填一次
            buff.position(old);         //回到最终位置
        }
    }

    private void writeChildren(WriteBuffer buff) {
        int len = keys.length;
        for (int i = 0; i <= len; i++) {
            buff.putLong(children[i].pos);
        }
    }

    /**
     * Store the page and update the position.
     *
     * @param c the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    //将page的内容和其他一些信息写入WriteBuffer里
    private int write(Chunk c, WriteBuffer buff) {
        int start = buff.position();
        int len = keys.length;
        int type = children != null ? 1     //1表示内部节点
                : 0;                        //0表示叶子
        buff.putInt(0).                     //先放一个固定4个字节的int的0，占个位置，后面会改为pageLength
                putShort((byte) 0).         //放一个固定2个字节的short的0，占个位置，后面会改为check
                putVarInt(map.getId()).     //放一个可变的，根据大小决定字节数的mapID
                putVarInt(len);             //放一个可变的keys.length
        int typePos = buff.position();
        buff.put((byte) type);              //放一个字节的type
        if (type == 1) {    //内部
            writeChildren(buff); //写所有children的pos，此时pagePos可能为0，在writeUnsavedRecursive中再回填一次
            for (int i = 0; i <= len; i++) { //keys.length + 1 才等于 children.length
                buff.putVarLong(children[i].count);
            }
        }
        map.getKeyType().write(buff, keys, len, true); //将keys里面每个key的长度与key的内容写入buff
        if (type == 0) {    //叶子
            map.getValueType().write(buff, values, len, false);  //将values里面每个value的长度与value的内容写入buff
        }
        int pageLength = buff.position() - start;                //得到pageLength
//        int chunkId = chunk.id;
        int chunkId = c.id;   //这个版本不引入chunk，先默认chunkId 为 0
        int check = DataUtil.getCheckValue(chunkId)
                ^ DataUtil.getCheckValue(start)
                ^ DataUtil.getCheckValue(pageLength);
        buff.putInt(start, pageLength).                         //写入pageLength
                putShort(start + 4, (short) check);
        if (pos != 0) {
            throw new RuntimeException("Page already stored");
        }
        pos = DataUtil.getPagePos(chunkId, start, pageLength, type); //得到pos
        return typePos + 1;       //返回type所放的位置加1
    }

    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
                     long filePos, long maxPos) {
        ByteBuffer buff;
        int length = DataUtil.getPageMaxLength(pos);
        buff = fileStore.readFully(filePos, length);
        Page p = new Page(map, 0);
        p.pos = pos;
        int chunkId = DataUtil.getPageChunkId(pos);
        int offset = DataUtil.getPageOffset(pos);
        p.read(buff, chunkId, offset, length);
        return p;
    }

    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int checkTest = DataUtil.getCheckValue(chunkId)
                ^ DataUtil.getCheckValue(offset)
                ^ DataUtil.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw new RuntimeException("File corrupted in chunk");
        }
        int mapId = DataUtil.readVarInt(buff);
        int len = DataUtil.readVarInt(buff);
        keys = new Object[len];
        int type = buff.get();
        boolean node = (type & 1) == DataUtil.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[len + 1];
            long[] p = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtil.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
        }
        map.getKeyType().read(buff, keys, len, true);
        if (!node) {
            values = new Object[len];
            map.getValueType().read(buff, values, len, false);
            totalCount = len;
        }
        recalculateMemory();
    }

    /*---------------------------------------------------------------------------------------------------------------*/
    public Object getKey(int index) {
        return keys[index];
    }

    public Page getChildPage(int index) {
//        return children[index].page;
        PageReference ref = children[index];
        return ref.page != null ? ref.page : map.readPage(ref.pos);
    }

    public Object getValue(int index) {
        return values[index];
    }

    public int getKeyCount() {
        return keys.length;
    }

    public boolean isLeaf() {
        return children == null;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public int getMemory() {
        return memory;
    }

    private void addMemory(int mem) {
        memory += mem;
    }

    void setVersion(long version) {
        this.version = version;
    }

    long getVersion() {
        return version;
    }

    public int getRawChildPageCount() {
        return children.length;
    }

    public long getPos() {
        return pos;
    }

    /*-----------------------------------------------------------------------------------------------------------*/
    public static class PageReference {
        final Page page;
        final long pos;
        final long count;

        public PageReference(Page page, long pos,long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }
    }

}
