package com.oron.wallet.financialrecon.service.helper;

import org.springframework.stereotype.Service;

@Service
public class ResourcePathRetriever {

    public String getResourcePath(String path) {
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResource(path).getPath();
    }
}
