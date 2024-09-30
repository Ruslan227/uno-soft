package org.example.transformers;

import org.example.tokenizer.ChunkTokenizer;

import java.nio.file.Path;

public abstract class AbstractFileWriter implements FileTransformer {
    protected static final int BUFFER_SIZE = 128;
    protected final Path inputFilePath;
    protected final Path outputFilePath;

    public AbstractFileWriter(Path inputFilePath, Path outputFilePath) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
    }

    public static long seekIndexByBufferIndex(
            long readerPosition,
            long fileSize,
            long bufferIndex,
            int bytesRead,
            ChunkTokenizer chunk) {
        var diff = readerPosition - bytesRead;
        return (diff >= 0) ? (diff + bufferIndex + 1) : (fileSize - bytesRead + chunk.getCurrentIndex());
    }
}
