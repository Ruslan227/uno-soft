package org.example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.*;

public class GroupAggregator extends AbstractWriter {
    private final FileInfo fileInfo;
    private final int[] parent;
    private final int[] size;

    /**
     * Contract: input file that contains matrix must be valid and transposed.
     *
     * @param inputFilePath  path to file that contains input transposed matrix
     * @param outputFilePath path to file that will contain output
     * @param fileInfo       information about file - line amount (now it is column amount as matrix is transposed).
     */
    public GroupAggregator(String inputFilePath, String outputFilePath, FileInfo fileInfo) {
        super(inputFilePath, outputFilePath);
        this.fileInfo = fileInfo;
        parent = new int[fileInfo.validLinesAmount()];
        IntStream.range(0, parent.length).forEach(i -> parent[i] = i);
        size = new int[fileInfo.validLinesAmount()];
        Arrays.fill(size, 1);
    }

    public void aggregateGroups() {
        Path absolutePath = Paths.get("").toAbsolutePath();
        var tmpFilePath = absolutePath.resolve("tmp.txt");
        var sortedTmpFilePath = absolutePath.resolve("tmp_sorted.txt");
        var outputChannelNotClosed = true;
        FileChannel fileOutputChannel = null;

        try (FileInputStream fis = new FileInputStream(inputFilePath);
             FileChannel fileInputChannel = fis.getChannel()) {
            fileOutputChannel = FileChannel.open(tmpFilePath, CREATE, WRITE, TRUNCATE_EXISTING);

            var buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int bytesRead;
            int curLineInd = 0;
            var isColumnEnd = true; // to add index for first line
            var columnPartBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            var isFirstTmpLine = true;

            String columnDelimiter = String.valueOf(ChunkTokenizer.getColumnDelimiter());

            while ((bytesRead = fileInputChannel.read(buffer)) != -1) {
                var chunk = new AggregatorChunkTokenizer(new String(buffer.array(), 0, bytesRead));

                while (chunk.hasRemainingCharacters()) {
                    var readColumnState = chunk.accumulateColumnValue(columnPartBuffer);

                    if (columnPartBuffer.position() > 0 && isColumnEnd) {
                        String index;
                        if (isFirstTmpLine) {
                            index = curLineInd + columnDelimiter;
                            isFirstTmpLine = false;
                        } else {
                            index = "\n" + curLineInd + columnDelimiter;
                        }
                        fileOutputChannel.write(ByteBuffer.wrap(index.getBytes()));
                    }

                    columnPartBuffer.flip();
                    while (columnPartBuffer.hasRemaining()) {
                        fileOutputChannel.write(columnPartBuffer);
                    }

                    if (readColumnState == AccumulateValueState.NEW_LINE) {
                        fileOutputChannel.close();
                        outputChannelNotClosed = false;
                        FileExternalSorter.sortBySecondColumn(tmpFilePath.toString(), sortedTmpFilePath.toString());
                        mergeGroupsBySameColumn(sortedTmpFilePath);
                        fileOutputChannel = FileChannel.open(tmpFilePath, CREATE, WRITE, TRUNCATE_EXISTING);
                        outputChannelNotClosed = true;
                        isColumnEnd = true;
                        isFirstTmpLine = true;
                        curLineInd = 0;
                    } else {
                        isColumnEnd = readColumnState == AccumulateValueState.DELIMITER;
                        if (isColumnEnd) {
                            curLineInd++;
                        }
                    }
                    columnPartBuffer.clear();
                }
                buffer.clear();
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (outputChannelNotClosed && fileOutputChannel != null) {
                try {
                    fileOutputChannel.close();
                } catch (IOException e) {
                    System.err.println("Failed to close the file channel: " + e.getMessage());
                }
            }

            try {
                Files.delete(tmpFilePath);
                Files.delete(sortedTmpFilePath);
            } catch (IOException e) {
                System.err.println("Failed to remove temporary files: " + e.getMessage());
            }
        }
    }

    private void mergeGroupsBySameColumn(Path sortedTmpFilePath) throws IOException {
        // assume that column value fits in RAM.
        try (var reader = new BufferedReader(new FileReader(sortedTmpFilePath.toFile()))) {
            String currentLine;
            var previous = IndexValue.from(reader.readLine());

            while ((currentLine = reader.readLine()) != null) {
                var current = IndexValue.from(currentLine);
                if (current.value().equals(previous.value())) {
                    mergeGroups(current.ind(), previous.ind());
                } else {
                    previous = current;
                }
            }
        }
    }

    private record IndexValue(int ind, String value) {
        private static final String DELIMITER = String.valueOf(ChunkTokenizer.getColumnDelimiter());

        public static IndexValue from(String s) {
            var words = s.split(DELIMITER, 2);
            int lineInd = Integer.parseInt(words[0]);
            var columnValue = words[1];

            return new IndexValue(lineInd, columnValue);
        }
    }

    private int findRoot(int ind) {
        if (ind == parent[ind]) {
            return ind;
        }
        parent[ind] = findRoot(parent[ind]);
        return parent[ind];
    }

    private void mergeGroups(int ind1, int ind2) {
        var r1 = findRoot(ind1);
        var r2 = findRoot(ind2);
        if (r1 != r2) {
            if (size[r1] < size[r2]) {
                parent[r1] = r2;
                size[r2] += size[r1];
            } else {
                parent[r2] = r1;
                size[r1] += size[r2];
            }
        }
    }

    private enum AccumulateValueState {
        NEW_LINE, DELIMITER, VALUE_PART
    }

    private static class AggregatorChunkTokenizer extends ChunkTokenizer {

        public AggregatorChunkTokenizer(String s) {
            super(s);
        }

        public AccumulateValueState accumulateColumnValue(ByteBuffer buffer) {
            if (isNewLine(s.charAt(curInd))) {
                curInd++;
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
                curInd++;
                return AccumulateValueState.NEW_LINE;
            }

            return AccumulateValueState.VALUE_PART;
        }
    }
}
