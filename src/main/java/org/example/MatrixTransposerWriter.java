package org.example;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MatrixTransposerWriter extends AbstractWriter {
    private final FileInfo fileInfo;
    private final long[] filePointers;

    public MatrixTransposerWriter(String inputFilePath, String outputFilePath, FileInfo fileInfo) {
        super(inputFilePath, outputFilePath);
        this.fileInfo = fileInfo;
        filePointers = new long[fileInfo.validLinesAmount()];
    }

    public void transpose() {
        saveFilePointers();

        try (RandomAccessFile raf = new RandomAccessFile(inputFilePath, "r");
             FileOutputStream fos = new FileOutputStream(outputFilePath);
             FileChannel fileWriterChannel = fos.getChannel()) {

            var buffer = new byte[BUFFER_SIZE];
            var columnPartBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            int bytesRead;
            var isColumnEnd = false;

            for (int curColumnInd = 0; curColumnInd < fileInfo.maxColumnsAmount(); curColumnInd++) {
                for (int lineInd = 0; lineInd < fileInfo.validLinesAmount(); lineInd++) {
                    raf.seek(filePointers[lineInd]);
                    isColumnEnd = false;

                    while (!isColumnEnd) {
                        bytesRead = raf.read(buffer);
                        if (bytesRead == -1) {
                            break;
                        }
                        var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));
                        var isColumnEndOptional = chunk.accumulateColumnValue(columnPartBuffer);
                        if (isColumnEndOptional.isEmpty()) { // end of line was already reached
                            break;
                        }
                        isColumnEnd = isColumnEndOptional.get();

                        if (!isColumnEnd) { // if EOF is not new line
                            isColumnEnd = bytesRead < BUFFER_SIZE && !chunk.hasRemainingCharacters();
                        }

                        if (isColumnEnd) {
                            filePointers[lineInd] = seekIndexByBufferIndex(
                                    raf.getFilePointer(),
                                    raf.length(),
                                    chunk.getCurrentIndex() - 1,
                                    bytesRead,
                                    chunk
                            );
                        }

                        columnPartBuffer.flip();
                        while (columnPartBuffer.hasRemaining()) {
                            fileWriterChannel.write(columnPartBuffer);
                        }
                        columnPartBuffer.clear();
                        if (isColumnEnd) {
                            fileWriterChannel.write(ByteBuffer.wrap(new byte[] {';'}));
                        }
                    }
                }
                fileWriterChannel.write(ByteBuffer.wrap(new byte[] {'\n'}));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveFilePointers() {
        try (FileInputStream fis = new FileInputStream(inputFilePath);
             FileChannel fileReaderChannel = fis.getChannel()) {

            filePointers[0] = 0;
            int lineInd = 1;

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int readBytes;

            while ((readBytes = fileReaderChannel.read(buffer)) > 0) {
                buffer.flip();

                ChunkTokenizer chunk = new ChunkTokenizer(UTF_8.decode(buffer).toString());

                while (chunk.hasRemainingCharacters()) {
                    int newLineInd = chunk.skipUtilNewValidLine();

                    if (newLineInd != -1) {
                        filePointers[lineInd] = seekIndexByBufferIndex(
                                fileReaderChannel.position(),
                                fileReaderChannel.size(),
                                newLineInd,
                                readBytes,
                                chunk
                        );
                        lineInd++;
                    }
                }

                buffer.clear();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long[] getFilePointers() {
        return filePointers;
    }
}
