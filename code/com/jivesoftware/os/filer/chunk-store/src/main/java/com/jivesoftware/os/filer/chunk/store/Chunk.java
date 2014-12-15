package com.jivesoftware.os.filer.chunk.store;

/**
 *
 */
public class Chunk<M> {

    final M monkey; // the monkey on our back
    final long chunkFP;
    final long startOfFP;
    final long endOfFP;
    long fp;

    public Chunk(M monkey, long chunkFP, long startOfFP, long endOfFP) {
        this.monkey = monkey;
        this.chunkFP = chunkFP;
        this.startOfFP = startOfFP;
        this.endOfFP = endOfFP;
    }
}
