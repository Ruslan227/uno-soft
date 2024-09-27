package org.example;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

import static java.lang.Math.max;


public class CorrectFileWriter {
    private static final int BUFFER_SIZE = 512 * 1024 * 1024;
    private final String inputFilePath;
    private final String outputFilePath;
    private int validLinesAmount = 0;
    private long maxColumnsAmount = 0;


    public CorrectFileWriter(String inputFilePath, String outputFilePath) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
    }

    private enum ReadingColumnState {
        DELIMITER,
        VALUE_WRAPPER_FIRST,
        VALUE,
        VALUE_WRAPPER_SECOND,
        NEW_LINE,
        SKIP_UNTIL_NEW_LINE
    }

    private Optional<ReadingColumnState> stateByFirstLexeme(ChunkTokenizer chunk) {
        chunk.skipWhitespaceIfPresent();
        var curCode = chunk.get();

        if (curCode == -1) {
            return Optional.empty();
        }

        var curSymb = (char) curCode;
        ReadingColumnState stateResult = null;

        if (chunk.isDigit(curSymb)) {
            stateResult = ReadingColumnState.VALUE;
        } else if (chunk.isNewLine(curSymb)) {
            stateResult = ReadingColumnState.NEW_LINE;
        } else if (chunk.isColumnDelimiter(curSymb)) {
            stateResult = ReadingColumnState.DELIMITER;
        } else if (chunk.isValueWrapper(curSymb)) {
            stateResult = ReadingColumnState.VALUE_WRAPPER_SECOND;
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
            var state = ReadingColumnState.DELIMITER;
            var prevState = state;

            while ((bytesRead = raf.read(buffer)) != -1) {
                var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));

                if (state != ReadingColumnState.SKIP_UNTIL_NEW_LINE && chunk.hasRemainingCharacters()) {
                    var optionalState = stateByFirstLexeme(chunk);
                    if (optionalState.isPresent()) {
                        if (state == optionalState.get() &&
                                state != ReadingColumnState.VALUE_WRAPPER_FIRST &&
                                state != ReadingColumnState.VALUE_WRAPPER_SECOND) {
                            state = prevState;
                        }
                    }
                }

                while (chunk.hasRemainingCharacters()) {
                    prevState = state;

                    switch (state) {
                        case VALUE -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = ReadingColumnState.VALUE_WRAPPER_SECOND;
                        }
                        case DELIMITER, NEW_LINE -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = ReadingColumnState.VALUE_WRAPPER_FIRST;
                        }
                        case VALUE_WRAPPER_FIRST -> {
                            isLineCorrect = chunk.skipDigits();
                            state = ReadingColumnState.VALUE;
                        }
                        case VALUE_WRAPPER_SECOND -> {
                            isLineCorrect = chunk.skipColumnDelimiter();
                            state = ReadingColumnState.DELIMITER;
                            if (!isLineCorrect) {
                                isLineCorrect = chunk.skipNewLines();
                                state = ReadingColumnState.NEW_LINE;
                            }
                        }
                        case SKIP_UNTIL_NEW_LINE -> {
                            int newLineInd = chunk.skipUtilNewValidLine();
                            if (newLineInd != -1) {
                                isLineCorrect = true;
                                state = ReadingColumnState.DELIMITER;
                                curLineStart = seekIndexByNewLineBufferIndex(raf, newLineInd);
                            }
                        }

                        default -> throw new RuntimeException("Unexpected state");
                    }

                    if (isLineCorrect && (state == ReadingColumnState.NEW_LINE ||
                            state == ReadingColumnState.VALUE_WRAPPER_SECOND && isEOF(bytesRead, buffer.length, chunk))) {
                        long nextLineStart = seekIndexByNewLineBufferIndex(raf, chunk.getCurrentIndex() - 1);
                        raf.seek(curLineStart);
                        addValidLineToOutputFile(raf, fileWriterChannel);
                        curLineStart = nextLineStart;
                        if (!isEOF(bytesRead, buffer.length, chunk)) {
                            raf.seek(curLineStart);
                        }
                        break;
                    } else if (!isLineCorrect) {
                        state = ReadingColumnState.SKIP_UNTIL_NEW_LINE;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private long seekIndexByNewLineBufferIndex(RandomAccessFile raf, int newLineIndex) throws IOException {
        return raf.getFilePointer() - BUFFER_SIZE + newLineIndex + 1;
    }

    private boolean isEOF(int bytesRead, int bufferLen, ChunkTokenizer chunk) {
        return bytesRead < bufferLen && !chunk.hasRemainingCharacters();
    }

    private void addValidLineToOutputFile(RandomAccessFile rafReader, FileChannel fileWriterChannel) throws IOException {
        var buffer = new byte[BUFFER_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead;
        var lineIsEnded = false;
        long delimitersAmount = 0;

        while (!lineIsEnded && (bytesRead = rafReader.read(buffer)) != -1) {
            var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));
            var newLineIndex = chunk.indexOfNewLine();

            if (newLineIndex == -1) {
                byteBuffer.put(buffer, 0, bytesRead);
                delimitersAmount += chunk.countColumnDelimiters();
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
    }

    public FileInfo fileInfo() {
        return new FileInfo(validLinesAmount, maxColumnsAmount);
    }

    public record FileInfo(int validLinesAmount, long maxColumnsAmount) {
    }
}
