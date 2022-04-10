package org.luitro.connector;

import org.luitro.connector.config.Selector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class Application {
    public static void main(String[] args) throws IOException {
        new Selector(Files.lines(Paths.get(args[0])).collect(Collectors.joining("\n")))
                .switchConnector();

        new Selector(Files.lines(Paths.get(args[1])).collect(Collectors.joining("\n")))
                .switchConnector();
    }
}
