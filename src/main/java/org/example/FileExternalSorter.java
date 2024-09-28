package org.example;

import com.google.code.externalsorting.ExternalSort;

import java.io.*;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileExternalSorter {

    public static void removeDuplicates(String inputFilePath, String outputFilePath) {
        try(var reader = new BufferedReader(new FileReader(inputFilePath))) {
            var inputFile = Paths.get(inputFilePath).toFile();
            var outputFile = Paths.get(outputFilePath).toFile();

            long maxMemoryUsage = 10 * 1024 * 1024;
            int maxTmpFiles = 500;

            List<File> sortedFiles = ExternalSort.sortInBatch(
                    reader,
                    inputFile.length(),
                    Comparator.naturalOrder(),
                    maxTmpFiles,
                    maxMemoryUsage,
                    UTF_8,
                    null,
                    false,
                    0,
                    false,
                    true
            );

            ExternalSort.mergeSortedFiles(sortedFiles, outputFile, Comparator.naturalOrder(), UTF_8, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
