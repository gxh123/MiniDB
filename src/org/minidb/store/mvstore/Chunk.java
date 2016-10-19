package org.minidb.store.mvstore;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by gxh on 2016/7/2.
 */
public class Chunk {
    /**
     * The maximum length of a chunk header, in bytes.
     */
    static final int MAX_HEADER_LENGTH = 1024;
    static final int FOOTER_LENGTH = 128;
    public final int id;
    public long block;     //这个Chunk起始block的编号
    public int len;        //block的数量
    public long version;   //这个chunk对应的mvstore的版本
    public long next;
    public int mapId;
    /**
     * The position of the meta root.
     */
    public long metaRootPos;

    Chunk(int id) {
        this.id = id;
    }

    /**
     * Read the header from the byte buffer.
     *
     * @param buff the source buffer
     * @param start the start of the chunk in the file
     * @return the chunk
     */
    static Chunk readChunkHeader(ByteBuffer buff, long start) {
        int pos = buff.position();
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        buff.get(data);
        try {
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '\n') {
                    // set the position to the start of the first page
                    buff.position(pos + i + 1);
                    String s = new String(data, 0, i, DataUtil.LATIN).trim();
                    return fromString(s);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("File corrupt reading chunk");
        }
        throw new RuntimeException("File corrupt reading chunk");
    }

    /**
     * Write the chunk header.
     *
     * @param buff the target buffer
     * @param minLength the minimum length
     */
    void writeChunkHeader(WriteBuffer buff, int minLength) {
        long pos = buff.position();
        buff.put(asString().getBytes(DataUtil.LATIN));
        while (buff.position() - pos < minLength - 1) { //用空格补够，要minLength - 1是因为下面要写入'\n'
            buff.put((byte) ' ');
        }
        buff.put((byte) '\n');
    }

    /**
     * Get the metadata key for the given chunk id.
     *
     * @param chunkId the chunk id
     * @return the metadata key
     */
    static String getMetaKey(int chunkId) {
        return "chunk." + Integer.toHexString(chunkId);
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    public static Chunk fromString(String s) {
        HashMap<String, String> map = DataUtil.parseMap(s);
        int id = DataUtil.readHexInt(map, "chunk", 0);
        Chunk c = new Chunk(id);
        c.block = DataUtil.readHexLong(map, "block", 0);
        c.len = DataUtil.readHexInt(map, "len", 0);
        c.metaRootPos = DataUtil.readHexLong(map, "root", 0);
        c.version = DataUtil.readHexLong(map, "version", id);
        c.next = DataUtil.readHexLong(map, "next", 0);
        c.mapId = DataUtil.readHexInt(map, "map", 0);
        return c;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk && ((Chunk) o).id == id;
    }

    /**
     * Get the chunk data as a string.
     *
     * @return the string
     */
    public String asString() {
        StringBuilder buff = new StringBuilder();
        DataUtil.appendMap(buff, "chunk", id);
        DataUtil.appendMap(buff, "block", block);
        DataUtil.appendMap(buff, "len", len);
        DataUtil.appendMap(buff, "map", mapId);
        DataUtil.appendMap(buff, "version", version);
        DataUtil.appendMap(buff, "root", metaRootPos);
        if (next != 0) {
            DataUtil.appendMap(buff, "next", next);
        }
        return buff.toString();
    }

    byte[] getFooterBytes() {
        StringBuilder buff = new StringBuilder();
        DataUtil.appendMap(buff, "chunk", id);
        DataUtil.appendMap(buff, "block", block);
        DataUtil.appendMap(buff, "version", version);
        byte[] bytes = buff.toString().getBytes(DataUtil.LATIN);
        int checksum = DataUtil.getFletcher32(bytes, bytes.length);
        DataUtil.appendMap(buff, "fletcher", checksum);
        while (buff.length() < Chunk.FOOTER_LENGTH - 1) {
            buff.append(' ');
        }
        buff.append("\n");
        return buff.toString().getBytes(DataUtil.LATIN);
    }

    @Override
    public String toString() {
        return asString();
    }
}

