package org.example.transformers;

import org.example.dto.FileInfo;
import org.example.exceptions.TransformerException;
import org.example.tokenizer.ChunkTokenizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

public class MatrixTransposer extends AbstractFileWriter {
    private final FileInfo fileInfo;
    private final long[] filePointers;

    public MatrixTransposer(Path inputFilePath, Path outputFilePath, FileInfo fileInfo) {
        super(inputFilePath, outputFilePath);
        this.fileInfo = fileInfo;
        filePointers = new long[fileInfo.validLinesAmount()];
    }

    @Override
    public Path transform(Path input) throws TransformerException {
        return transpose();
    }

    public Path transpose() throws TransformerException {
        saveFilePointers();

        try (RandomAccessFile raf = new RandomAccessFile(inputFilePath.toString(), "r");
             FileChannel fileWriterChannel = FileChannel.open(outputFilePath, TRUNCATE_EXISTING, WRITE, CREATE)) {

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
        } catch (FileNotFoundException e) {
            throw new TransformerException("Failed to find file when transposing matrix.", inputFilePath, outputFilePath, e);
        } catch (IOException e) {
            throw new TransformerException("Failed to transpose the matrix. File: " + inputFilePath, e);
        }

        return outputFilePath;
    }

    private void saveFilePointers() throws TransformerException {
        try (FileChannel fileReaderChannel = FileChannel.open(inputFilePath, READ)) {

            filePointers[0] = 0;
            int lineInd = 1;

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int readBytes;

            while ((readBytes = fileReaderChannel.read(buffer)) > 0) {
                ChunkTokenizer chunk = new ChunkTokenizer(new String(buffer.array(), 0, readBytes));

                while (chunk.hasRemainingCharacters()) {
                    int newLineInd = chunk.skipUtilNewValidLine();

                    if (newLineInd != -1 && lineInd < filePointers.length) {
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
            throw new TransformerException("Failed to count file pointers for matrix transposing in file: " + inputFilePath, e);
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
