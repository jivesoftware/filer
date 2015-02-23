package com.jivesoftware.os.filer.io.chunk;

/**
 *
 */
public class Chunk<M> {

    final M monkey; // the monkey on our back
    final long chunkFP;
    final long startOfFP;
    final long endOfFP;
    transient long acquisitions;

    public Chunk(M monkey, long chunkFP, long startOfFP, long endOfFP) {
        this.monkey = monkey;
        this.chunkFP = chunkFP;
        this.startOfFP = startOfFP;
        this.endOfFP = endOfFP;
    }
}
