package com.decathlon.onebillionrowchallenge.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class UsingBufferedReader extends DataReader {

    private BufferedReader bufferedReader;

    public UsingBufferedReader(File file) throws Exception {
        this.bufferedReader = new BufferedReader(new FileReader(file));
    }

    @Override
    public void read() throws IOException {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            final var currentValue = Double.valueOf(line.split(",")[1]);
            replaceMinIfNeeded(currentValue);
            replaceMaxIfNeeded(currentValue);
        }
    }
}
