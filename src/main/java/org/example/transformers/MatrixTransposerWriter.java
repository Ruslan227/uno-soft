package org.example.transformers;

import org.example.dto.FileInfo;
import org.example.tokenizer.ChunkTokenizer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MatrixTransposerWriter extends AbstractFileWriter {
    private final FileInfo fileInfo;
    private final long[] filePointers;

    public MatrixTransposerWriter(Path inputFilePath, Path outputFilePath, FileInfo fileInfo) {
        super(inputFilePath, outputFilePath);
        this.fileInfo = fileInfo;
        filePointers = new long[fileInfo.validLinesAmount()];
    }

    public void transpose() {
        saveFilePointers();

        try (RandomAccessFile raf = new RandomAccessFile(inputFilePath.toString(), "r");
             FileOutputStream fos = new FileOutputStream(outputFilePath.toString());
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
                        var chunk = new MatrixChunkTokenizer(new String(buffer, 0, bytesRead));
                        var readPartState = chunk.accumulateColumnValue(columnPartBuffer);
                        if (readPartState == AccumulateValueState.NEW_LINE && !columnPartBuffer.hasRemaining()) {
                            fileWriterChannel.write(ByteBuffer.wrap(new byte[]{(byte) ChunkTokenizer.getColumnDelimiter()}));
                            break;
                        }
                        isColumnEnd = readPartState == AccumulateValueState.DELIMITER || readPartState == AccumulateValueState.NEW_LINE;

                        var isEOF = bytesRead < BUFFER_SIZE && !chunk.hasRemainingCharacters();
                        isColumnEnd = isColumnEnd || isEOF; // if EOF is not new line

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
                            fileWriterChannel.write(ByteBuffer.wrap(new byte[]{(byte) ChunkTokenizer.getColumnDelimiter()}));
                        }
                    }
                }
                fileWriterChannel.write(ByteBuffer.wrap(new byte[]{'\n'}));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveFilePointers() {
        try (FileInputStream fis = new FileInputStream(inputFilePath.toString());
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

                    if (newLineInd != -1 && chunk.hasRemainingCharacters()) {
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

    private enum AccumulateValueState {
        NEW_LINE, DELIMITER, VALUE_PART
    }

    private static class MatrixChunkTokenizer extends ChunkTokenizer {

        public MatrixChunkTokenizer(String s) {
            super(s);
        }

        public AccumulateValueState accumulateColumnValue(ByteBuffer buffer) {
            if (isNewLine(s.charAt(curInd))) {
                return AccumulateValueState.NEW_LINE;
            }
            if (isColumnDelimiter(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.DELIMITER;
            }
            while (hasRemainingCharacters() && !isColumnDelimiter(s.charAt(curInd)) && !isNewLine(s.charAt(curInd))) {
                buffer.put((byte) s.charAt(curInd));
                curInd++;
            }
            if (hasRemainingCharacters() && isColumnDelimiter(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.DELIMITER;
            }
            if (hasRemainingCharacters() && isNewLine(s.charAt(curInd))) {
                return AccumulateValueState.NEW_LINE;
            }

            return AccumulateValueState.VALUE_PART;
        }
    }
}
