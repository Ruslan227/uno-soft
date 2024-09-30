package org.example.transformers;

import org.example.exceptions.TransformerException;

import java.nio.file.Path;

public interface FileTransformer {
    /**
     * Reads the input file, transforms its data, and writes the result to the output file.
     *
     * @param input the path of the file to read.
     * @return the path of the output file.
     * @throws TransformerException if the data transformation fails during reading, transforming, or writing.
     */
    Path transform(Path input) throws TransformerException;
}
