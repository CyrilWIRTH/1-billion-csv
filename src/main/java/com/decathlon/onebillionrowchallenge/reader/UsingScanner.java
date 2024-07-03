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
            final var currentValue = Double.valueOf(line.split(",")[1]);
            replaceMinIfNeeded(currentValue);
            replaceMaxIfNeeded(currentValue);
        }
    }
}
