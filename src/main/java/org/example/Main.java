package org.example;

import org.example.external.sort.ExternalFileSorter;
import org.example.transformers.GroupAggregator;
import org.example.transformers.MatrixTransposer;
import org.example.transformers.ValidLineFilter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected 1 argument containing path to input file.");
        }

        var correctFileWriterOutputPath = Path.of("src", "main", "resources", "output_correct_writer.txt");
        var correctFileWriter = new ValidLineFilter(
                Path.of(args[0]),
                correctFileWriterOutputPath
        );
        correctFileWriter.filter();

        var duplicateRemovalOutputPath = Path.of("src", "main", "resources", "output_remove_duplicates.txt");
        ExternalFileSorter.removeDuplicates(correctFileWriterOutputPath, duplicateRemovalOutputPath);

        Files.delete(correctFileWriterOutputPath);

        var matrixTransposerWriterOutputPath = Path.of("src", "main", "resources", "output_matrix_transposer.txt");
        var matrixTransposerWriter = new MatrixTransposer(
                duplicateRemovalOutputPath,
                matrixTransposerWriterOutputPath,
                correctFileWriter.fileInfo()
        );
        matrixTransposerWriter.transpose();

        var output = Path.of("src", "main", "resources", "final_out.txt");
        var groupAggregator = new GroupAggregator(
                matrixTransposerWriterOutputPath,
                output,
                correctFileWriter.fileInfo(),
                duplicateRemovalOutputPath
        );
        groupAggregator.aggregateGroups();

        Files.delete(matrixTransposerWriterOutputPath);
        Files.delete(duplicateRemovalOutputPath);
    }
}
