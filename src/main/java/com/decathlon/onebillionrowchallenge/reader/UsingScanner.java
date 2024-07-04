package com.decathlon.onebillionrowchallenge.reader;

import java.io.File;
import java.util.Scanner;

public class UsingScanner extends DataReader {
    private Scanner scanner;

    public UsingScanner(File file) throws Exception {
        this.scanner = new Scanner(file);
    }

    @Override
    public void read() {
        while(scanner.hasNextLine()) {
            final var line = scanner.nextLine();
            var d = line.split(";");
            final var currentValue = Double.valueOf(d[1]);
            addData(d[0], currentValue);
        }
    }
}
