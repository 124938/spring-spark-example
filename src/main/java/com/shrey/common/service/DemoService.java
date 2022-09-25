package com.shrey.common.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DemoService {

    private static final Logger logger = LoggerFactory.getLogger(DemoService.class);

    @Value("${app.name}")
    private String name;

    @Value("${app.description}")
    private String description;

    public void print() {
        logger.info("Application name : {}", this.name);
        logger.info("Application description : {}", this.description);
    }

}
