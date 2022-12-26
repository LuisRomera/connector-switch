package org.luitro.connector.config;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class RocksDbConfig {

    private static final Logger log = LoggerFactory.getLogger(RocksDbConfig.class);

    @Bean
    public RocksDBData rocksDB() {
        return new RocksDBData();
    }

}
