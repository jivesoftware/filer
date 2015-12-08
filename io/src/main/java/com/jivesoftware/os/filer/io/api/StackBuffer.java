package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFilerDuplicateBuffer;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.chunk.Chunk;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;

/**
 *
 * @author jonathan.colt
 */
public class StackBuffer {

    public final byte[] primitiveBuffer = new byte[8];
    private final IBA accessKey = new IBA(primitiveBuffer);
    private int chunkFilerStackDepth = 0;
    private ChunkFiler[] chunkFilers = new ChunkFiler[8];
    public final AutoGrowingByteBufferBackedFilerDuplicateBuffer duplicateBuffer = new AutoGrowingByteBufferBackedFilerDuplicateBuffer(1024);

    private int chunkyStackDepth = 0;
    private Chunky[] chunkys = new Chunky[8];

    public IBA accessKey(byte[] key) {
        accessKey.violateImmutability(key);
        return accessKey;
    }

    public ChunkFiler chunkFiler(ChunkStore chunkStore, AutoGrowingByteBufferBackedFiler filer, long chunkFP, long startOfFP, long endOfFP) {
        ChunkFiler chunkFiler;
        if (chunkFilerStackDepth > 0 && chunkFilers[chunkFilerStackDepth - 1] != null) {
            chunkFilerStackDepth--;
            chunkFiler = chunkFilers[chunkFilerStackDepth];
            chunkFilers[chunkFilerStackDepth] = null;
            chunkFiler.mutate(chunkStore, filer, chunkFP, startOfFP, endOfFP);
        } else {
            chunkFiler = new ChunkFiler(chunkStore, filer, chunkFP, startOfFP, endOfFP);
        }
        return chunkFiler;
    }

    public void recycle(ChunkFiler chunkFiler) {
        if (chunkFilerStackDepth < chunkFilers.length) {
            chunkFiler.mutate(null, null, -1, -1, -1);
            chunkFilers[chunkFilerStackDepth] = chunkFiler;
            chunkFilerStackDepth++;
        }
    }

    public <M> Chunky<M> chunky(AutoGrowingByteBufferBackedFiler duplicate, ChunkFiler filer, Chunk<M> monkey) {
        Chunky<M> chunky;
        if (chunkyStackDepth > 0 && chunkys[chunkyStackDepth - 1] != null) {
            chunkyStackDepth--;
            chunky = chunkys[chunkyStackDepth];
            chunkys[chunkyStackDepth] = null;
            chunky.mutate(duplicate, filer, monkey);
        } else {
            chunky = new Chunky<M>(duplicate, filer, monkey);
        }
        return chunky;
    }

    public <M> void recycle(Chunky<M> chunky) {
        if (chunkyStackDepth < chunkFilers.length) {
            chunky.mutate(null, null,null);
            chunkys[chunkyStackDepth] = chunky;
            chunkyStackDepth++;
        }
    }

    public static class Chunky<M> {

        public AutoGrowingByteBufferBackedFiler duplicate;
        public ChunkFiler filer;
        public Chunk<M> monkey;

        Chunky(AutoGrowingByteBufferBackedFiler duplicate, ChunkFiler filer, Chunk<M> monkey) {
            this.duplicate = duplicate;
            this.filer = filer;
            this.monkey = monkey;
        }

        public void mutate(AutoGrowingByteBufferBackedFiler duplicate, ChunkFiler filer, Chunk<M> monkey) {
            this.duplicate = duplicate;
            this.filer = filer;
            this.monkey = monkey;
        }
    }
}
