package org.example;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

import static java.lang.Math.max;
import static org.example.ColumnState.*;

public class CorrectFileWriter extends AbstractWriter {
    private int validLinesAmount = 0;
    private long maxColumnsAmount = 0;

    public CorrectFileWriter(String inputFilePath, String outputFilePath) {
        super(inputFilePath, outputFilePath);
    }

    private Optional<ColumnState> stateByFirstLexeme(ChunkTokenizer chunk) {
        chunk.skipWhitespaceIfPresent();
        var curCode = chunk.get();

        if (curCode == -1) {
            return Optional.empty();
        }

        var curSymb = (char) curCode;
        ColumnState stateResult = null;

        if (chunk.isDigit(curSymb)) {
            stateResult = VALUE;
        } else if (chunk.isNewLine(curSymb)) {
            stateResult = NEW_LINE;
        } else if (chunk.isColumnDelimiter(curSymb)) {
            stateResult = DELIMITER;
        } else if (chunk.isValueWrapper(curSymb)) {
            stateResult = VALUE_WRAPPER_SECOND;
        }

        if (stateResult == null) {
            return Optional.empty();
        }

        return Optional.of(stateResult);
    }

    public void writeOutput() {
        try (RandomAccessFile raf = new RandomAccessFile(inputFilePath, "r");
             FileOutputStream fos = new FileOutputStream(outputFilePath);
             FileChannel fileWriterChannel = fos.getChannel()) {

            long curLineStart = raf.getFilePointer();
            var buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            var isLineCorrect = true;
            var state = DELIMITER;
            var prevState = state;

            while ((bytesRead = raf.read(buffer)) != -1) {
                var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));

                if (state != SKIP_UNTIL_NEW_LINE && chunk.hasRemainingCharacters()) {
                    var optionalState = stateByFirstLexeme(chunk);
                    if (optionalState.isPresent()) {
                        if (state == optionalState.get() &&
                                state != VALUE_WRAPPER_FIRST && state != VALUE_WRAPPER_SECOND) {
                            state = prevState;
                        }
                    }
                }

                while (chunk.hasRemainingCharacters()) {
                    prevState = state;

                    switch (state) {
                        case VALUE -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = VALUE_WRAPPER_SECOND;
                        }
                        case DELIMITER -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = VALUE_WRAPPER_FIRST;
                        }
                        case VALUE_WRAPPER_FIRST -> {
                            isLineCorrect = chunk.skipDigits();
                            state = VALUE;
                        }
                        case VALUE_WRAPPER_SECOND -> {
                            isLineCorrect = chunk.skipColumnDelimiter();
                            state = DELIMITER;
                            if (!isLineCorrect) {
                                isLineCorrect = chunk.skipNewLines();
                                state = NEW_LINE;
                            }
                        }
                        case SKIP_UNTIL_NEW_LINE -> {
                            int newLineInd = chunk.skipUtilNewValidLine();
                            if (newLineInd != -1) {
                                isLineCorrect = true;
                                state = DELIMITER;
                                curLineStart = seekIndexByBufferIndex(raf.getFilePointer(), raf.length(), newLineInd, bytesRead, chunk);
                            }
                        }

                        default -> throw new RuntimeException("Unexpected state");
                    }

                    if (isLineCorrect && (state == NEW_LINE ||
                            state == VALUE_WRAPPER_SECOND && isEOF(bytesRead, buffer.length, chunk))) {
                        raf.seek(curLineStart);
                        curLineStart = addValidLineToOutputFile(raf, fileWriterChannel);
                        if (!isEOF(bytesRead, buffer.length, chunk)) {
                            raf.seek(curLineStart);
                        }
                        state = SKIP_UNTIL_NEW_LINE;
                        break;
                    } else if (!isLineCorrect) {
                        state = SKIP_UNTIL_NEW_LINE;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private boolean isEOF(int bytesRead, int bufferLen, ChunkTokenizer chunk) {
        return bytesRead < bufferLen && !chunk.hasRemainingCharacters();
    }

    private long addValidLineToOutputFile(RandomAccessFile rafReader, FileChannel fileWriterChannel) throws IOException {
        var buffer = new byte[BUFFER_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead;
        var lineIsEnded = false;
        long delimitersAmount = 0;
        long filePointerToBufferWithNewLine = rafReader.getFilePointer();

        while (!lineIsEnded && (bytesRead = rafReader.read(buffer)) != -1) {
            var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));
            var newLineIndex = chunk.indexOfNewLine();

            if (newLineIndex == -1) {
                byteBuffer.put(buffer, 0, bytesRead);
                delimitersAmount += chunk.countColumnDelimiters();
                filePointerToBufferWithNewLine = rafReader.getFilePointer();
            } else {
                lineIsEnded = true;
                byteBuffer.put(buffer, 0, newLineIndex + 1);
                delimitersAmount += chunk.countColumnDelimitersWithRightBorder(newLineIndex);
            }

            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                fileWriterChannel.write(byteBuffer);
            }
            byteBuffer.clear();
        }

        validLinesAmount++;
        maxColumnsAmount = max(maxColumnsAmount, delimitersAmount + 1);

        return filePointerToBufferWithNewLine;
    }

    public String outputFilePath() {
        return outputFilePath;
    }

    public FileInfo fileInfo() {
        return new FileInfo(validLinesAmount, maxColumnsAmount);
    }
}
