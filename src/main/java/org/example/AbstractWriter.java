package org.example;

public abstract class AbstractWriter {
    protected static final int BUFFER_SIZE = 256 * 1024 * 1024; // 256 Mb
    protected final String inputFilePath;
    protected final String outputFilePath;

    public AbstractWriter(String inputFilePath, String outputFilePath) {
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
