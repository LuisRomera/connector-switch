package org.luitro.connector;

import org.luitro.connector.config.Selector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application {
    public static void main(String[] args) throws IOException {

        SpringApplication app = new SpringApplication(Application.class);
        Environment env = app.run(args).getEnvironment();

        //        new Selector(Files.lines(Paths.get(args[0])).collect(Collectors.joining("\n")))
//                .switchConnector();
//
//        new Selector(Files.lines(Paths.get(args[1])).collect(Collectors.joining("\n")))
//                .switchConnector();
    }
}
