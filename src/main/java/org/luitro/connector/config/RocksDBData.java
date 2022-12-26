package org.luitro.connector.config;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RocksDBData {

    private static final Logger log = LoggerFactory.getLogger(RocksDBData.class);

    private final HashMap<String, RocksDB> dbs = new HashMap<>();
    public RocksDBData() {

        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);

        List<String> listDB = new ArrayList<>();
        listDB.add("elastic-kafka");

        listDB.forEach(name -> {
            File dbDir = new File("C:\\tmp\\rocks-db", name);
            try {
                Files.createDirectories(dbDir.getParentFile().toPath());
                Files.createDirectories(dbDir.getAbsoluteFile().toPath());
                RocksDB db = RocksDB.open(options, dbDir.getAbsolutePath());
                dbs.put(name, db);
            } catch (IOException | RocksDBException ex) {
                log.error("Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
                        ex.getCause(), ex.getMessage(), ex.getStackTrace());
            }
            log.info("RocksDB initialized and ready to use");
        });

    }


    public HashMap<String, RocksDB> getDbs() {
        return dbs;
    }
}
