package org.luitro.connector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.luitro.connector.config.RocksDbConfig;
import org.luitro.connector.repository.RocksDBRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = Application.class)public class RocksDbTest {


//    @Autowired
//    private RocksDbConfig rocksDbConfig;

    @Autowired
    private RocksDBRepository repository;

    @Test
    public void consumerTest() throws IOException {
//        new Selector(Files.lines(Paths.get("C:\\Users\\larom\\proyectos\\connector-switch\\src\\test\\resources\\elastic-kafka.json")).collect(Collectors.joining("\n")))
//                .switchConnector();
//        rocksDbConfig.rocksDB();
        repository.save("elastic-kafka", "key", "value");
//
        String value = repository.find("elastic-kafka", "key");

        System.out.println("as");
    }
}
