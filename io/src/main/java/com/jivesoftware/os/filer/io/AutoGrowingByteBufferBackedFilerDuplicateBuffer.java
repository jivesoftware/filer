package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class AutoGrowingByteBufferBackedFilerDuplicateBuffer {

    private int autoGrowingFilersStackDepth = 0;
    private AutoGrowingByteBufferBackedFiler[] autoGrowingFilers;

    public AutoGrowingByteBufferBackedFilerDuplicateBuffer(int maxBuffers) {
        autoGrowingFilers = new AutoGrowingByteBufferBackedFiler[maxBuffers];
        byteBufferBackedFilers = new ByteBufferBackedFiler[maxBuffers];
    }

    public AutoGrowingByteBufferBackedFiler duplicate(
        AutoGrowingByteBufferBackedFilerDuplicateBuffer duplicateBuffer,
        ByteBufferBackedFiler[] filers,
        int filersLength,
        long maxBufferSegmentSize,
        int fShift,
        long fseekMask,
        long length,
        long startFP,
        long endFp
    ) {
        AutoGrowingByteBufferBackedFiler autoGrowingFiler;
        if (autoGrowingFilersStackDepth > 0 && autoGrowingFilers[autoGrowingFilersStackDepth - 1] != null) {
            autoGrowingFilersStackDepth--;
            autoGrowingFiler = autoGrowingFilers[autoGrowingFilersStackDepth];
            autoGrowingFilers[autoGrowingFilersStackDepth] = null;

            // WHY doesn't this work?
            //ByteBufferBackedFiler[] duplicate = autoGrowingFiler.filers.length <= filersLength ? autoGrowingFiler.filers : new ByteBufferBackedFiler[filersLength];
            ByteBufferBackedFiler[] duplicate = new ByteBufferBackedFiler[filersLength];
            duplicate(duplicate, filersLength, maxBufferSegmentSize, startFP, endFp, filers);
            autoGrowingFiler.mutate(maxBufferSegmentSize, duplicate, filersLength, length, fShift, fseekMask);

        } else {
            ByteBufferBackedFiler[] duplicate = new ByteBufferBackedFiler[filersLength];
            duplicate(duplicate, filersLength, maxBufferSegmentSize, startFP, endFp, filers);
            autoGrowingFiler = new AutoGrowingByteBufferBackedFiler(maxBufferSegmentSize, duplicate, filersLength, length, fShift, fseekMask);
        }
        return autoGrowingFiler;
    }

    private void duplicate(ByteBufferBackedFiler[] duplicate,
        int duplicateLength,
        long maxBufferSegmentSize,
        long startFP,
        long endFp,
        ByteBufferBackedFiler[] filers) {
        for (int i = 0; i < duplicateLength; i++) {
            if ((i + 1) * maxBufferSegmentSize < startFP || (i - 1) * maxBufferSegmentSize > endFp) {
                continue;
            }
            duplicate[i] = byteBufferFiler(filers[i].buffer.duplicate());
        }
    }

    public void recycle(AutoGrowingByteBufferBackedFiler recycle) {
        if (autoGrowingFilersStackDepth < autoGrowingFilers.length) {
            for (int i = 0; i < recycle.filers.length; i++) {
                if (recycle.filers[i] != null) {
                    recycle(recycle.filers[i]);
                    recycle.filers[i] = null;
                }
            }
            recycle.mutate(-1, recycle.filers, -1, -1, -1, -1);
            autoGrowingFilers[autoGrowingFilersStackDepth] = recycle;
            autoGrowingFilersStackDepth++;
        }
    }

    private int byteBufferBackedFilersStackDepth = 0;
    private ByteBufferBackedFiler[] byteBufferBackedFilers = new ByteBufferBackedFiler[2];

    public ByteBufferBackedFiler byteBufferFiler(ByteBuffer buffer) {
        ByteBufferBackedFiler byteBufferBackedFiler;
        if (byteBufferBackedFilersStackDepth > 0 && byteBufferBackedFilers[byteBufferBackedFilersStackDepth - 1] != null) {
            byteBufferBackedFilersStackDepth--;
            byteBufferBackedFiler = byteBufferBackedFilers[byteBufferBackedFilersStackDepth];
            byteBufferBackedFilers[byteBufferBackedFilersStackDepth] = null;
            byteBufferBackedFiler.mutate(buffer);
        } else {
            byteBufferBackedFiler = new ByteBufferBackedFiler(buffer);
        }
        return byteBufferBackedFiler;
    }

    public void recycle(ByteBufferBackedFiler recycle) {
        if (byteBufferBackedFilersStackDepth + 1 == byteBufferBackedFilers.length) {
            ByteBufferBackedFiler[] newByteBufferBackedFilers = new ByteBufferBackedFiler[byteBufferBackedFilers.length * 2];
            System.arraycopy(byteBufferBackedFilers, 0, newByteBufferBackedFilers, 0, byteBufferBackedFilers.length);
            byteBufferBackedFilers = newByteBufferBackedFilers;
        }
        if (byteBufferBackedFilersStackDepth < byteBufferBackedFilers.length) {
            recycle.mutate(null);
            byteBufferBackedFilers[byteBufferBackedFilersStackDepth] = recycle;
            byteBufferBackedFilersStackDepth++;
        }
    }
}
