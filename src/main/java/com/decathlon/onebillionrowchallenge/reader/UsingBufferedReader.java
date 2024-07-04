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
            var d = line.split(";");
            final var currentValue = Double.valueOf(d[1]);
            addData(d[0], currentValue);
        }
    }
}
