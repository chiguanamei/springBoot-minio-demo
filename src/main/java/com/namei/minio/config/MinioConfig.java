package com.namei.minio.config;


import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author: namei
 * @date: 2021/3/29
 * @description:
 */
@ConfigurationProperties(prefix = "minio")
public class MinioConfig {

    private String endpoint;

    private Integer port;

    private String accessKey;

    private String secretKey;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
}
