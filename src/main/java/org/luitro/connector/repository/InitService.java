package org.luitro.connector.repository;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class InitService {


    @Autowired
    private RocksDBRepository repository;


    @PostConstruct
    public void startInit() throws Exception {


        repository.save("elastic-kafka", "key", "value");
        throw new Exception();
//
//        String value = repository.find("elastic-kafka", "key");
//
//        System.out.println("as");

    }
}
