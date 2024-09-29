package org.example;

import com.google.code.externalsorting.ExternalSort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileExternalSorter {
    private static final long MAX_MEMORY_USAGE = 256 * 1024 * 1024; // 256 Mb
    private static final int MAX_TMP_FILES = 500;

    public static void removeDuplicates(String inputFilePath, String outputFilePath) {
        abstractSort(inputFilePath, outputFilePath, Comparator.naturalOrder(), true);
    }

    public static void sortBySecondColumn(String inputFilePath, String outputFilePath) {
        Comparator<String> cmp = (s1, s2) -> {
            var columnDelimiter = String.valueOf(ChunkTokenizer.getColumnDelimiter());
            var column1 = s1.split(columnDelimiter, 2)[1];
            var column2 = s2.split(columnDelimiter, 2)[1];
            return column1.compareTo(column2);
        };
        abstractSort(inputFilePath, outputFilePath, cmp, false);
    }

    public static void sortByGroup(String inputFilePath, String outputFilePath, String delimiter, int[] size) {
        Comparator<String> cmp = (s1, s2) -> {
            var root1 = Integer.parseInt(s1.split(delimiter, 2)[0]);
            var root2 = Integer.parseInt(s2.split(delimiter, 2)[0]);
            if (root1 != root2) {
               return size[root2] - size[root1];
            } else {
                return 0;
            }
        };
        abstractSort(inputFilePath, outputFilePath, cmp, false);
    }

    private static void abstractSort(String inputFilePath, String outputFilePath, Comparator<String> cmp, boolean distinct) {
        try (var reader = new BufferedReader(new FileReader(inputFilePath))) {
            var inputFile = Paths.get(inputFilePath).toFile();
            var outputFile = Paths.get(outputFilePath).toFile();

            List<File> sortedFiles = ExternalSort.sortInBatch(
                    reader,
                    inputFile.length(),
                    cmp,
                    MAX_TMP_FILES,
                    MAX_MEMORY_USAGE,
                    UTF_8,
                    null,
                    distinct,
                    0,
                    false,
                    true
            );

            ExternalSort.mergeSortedFiles(sortedFiles, outputFile, cmp, UTF_8, distinct);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
