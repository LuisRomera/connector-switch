package org.luitro.connector.repository;

import org.luitro.connector.config.RocksDBData;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RocksDBRepository {

    private static final Logger log = LoggerFactory.getLogger(RocksDBRepository.class);



    @Autowired
    private RocksDBData rocksDBData;

    public synchronized void save(String dbName, String key, String value) {
        log.info("save");
        try {
            rocksDBData.getDbs().get(dbName).put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            log.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }

    public String find(String dbName, String key) {
        log.info("find");
        String result = null;
        try {
            byte[] bytes = rocksDBData.getDbs().get(dbName).get(key.getBytes());
            if (bytes == null) return null;
            result = new String(bytes);
        } catch (RocksDBException e) {
            log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
        }
        return result;
    }

    public void delete(String dbName, String key) {
        log.info("delete");
        try {
            rocksDBData.getDbs().get(dbName).delete(key.getBytes());
        } catch (RocksDBException e) {
            log.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
        }
    }
}
