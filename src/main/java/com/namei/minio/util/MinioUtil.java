package com.namei.minio.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.namei.minio.config.MinioConfig;
import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.SimpleTriggerContext;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author: namei
 * @date: 2021/3/29
 * @description:
 */
@Configuration
@EnableConfigurationProperties({MinioConfig.class})
public class MinioUtil {

    @Autowired
    private MinioConfig minioConfig;

    private MinioClient minioClient = null;

    @PostConstruct
    private void initMinioClient() {
        if(null == minioClient) {
            minioClient = MinioClient.builder()
                    .endpoint(minioConfig.getEndpoint(), minioConfig.getPort(), false)
                    .credentials(minioConfig.getAccessKey(), minioConfig.getSecretKey())
                    .build();
        }

    }

    /**
     * @method: 通过文件流上传文件
     * @param bucketName:       桶名称
     * @param fileName:         文件名称
     * @param is:               文件输入流
     * @param contentType:      文件类型
     * @return: java.lang.String
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public String putObject(String bucketName, String fileName, InputStream is, String contentType) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InsufficientDataException, ErrorResponseException {

        checkBucket(bucketName);

        /**
         * Minio无法系统的创建多级目录，若需要多级目录，则可以在上传时指定objectName为多级
         * 例如： bucketName: test
         *       objectName: 20210329/test.txt
         *  则会在桶 test 下创建 20210329 文件夹，test.txt 位于文件夹 20210329 内
         */

        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now());
        String objectName = date + "/" + fileName;

        /**
         *  通过文件流上传文件时要注意指定 contentType ，否则会使用默认的 contentType（application/octet-stream），会导致文件如法预览，只能下载
         */
        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .contentType(contentType)
                        .stream(is, is.available(), -1)
                        .build());

        //获取文件限时链接（通过 expiry 设置）
        String expiryUrl = minioClient.getPresignedObjectUrl(
                   GetPresignedObjectUrlArgs.builder()
                       .method(Method.GET)
                       .bucket(bucketName)
                       .object(objectName)
                       .expiry(60, TimeUnit.MINUTES)
                       .build());

        //拼接永久链接
        String url = minioConfig.getEndpoint() + ":" + minioConfig.getPort() + "/" + bucketName + "/" + objectName;

        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        map.put("expiryUrl", expiryUrl);

        return new ObjectMapper().writeValueAsString(map);
    }

    /**
     * @method: 通过文件上传文件
     * @param bucketName:
     * @param fileName:
     * @return: java.lang.String
     * @author: weiwenbin
     * @date: 2021/3/29
     * @description:
     */
    public String uploadObject(String bucketName, String fileName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InsufficientDataException, ErrorResponseException {

        checkBucket(bucketName);

        fileName = StringUtils.isBlank(fileName) ? "99.jpg" : fileName;
        /**
         * Minio无法系统的创建多级目录，若需要多级目录，则可以在上传时指定objectName为多级
         * 例如： bucketName: test
         *       objectName: 20210329/test.txt
         *  则会在桶 test 下创建 20210329 文件夹，test.txt 位于文件夹 20210329 内
         */

        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now());
        String objectName = date + "/" + fileName;

        /**
         *  通过uploadObject上传文件时若不指定contentType，则会默认为文件实际的contentType，若手动指定contentType则会已指定的为准
         */
        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .filename(fileName)
                        //.contentType("application/octet-stream")
                        .build());

        //获取文件限时链接（通过 expiry 设置）
        String expiryUrl = minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(60, TimeUnit.MINUTES)
                        .build());

        //拼接永久链接
        String url = minioConfig.getEndpoint() + ":" + minioConfig.getPort() + "/" + bucketName + "/" + objectName;

        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        map.put("expiryUrl", expiryUrl);

        return new ObjectMapper().writeValueAsString(map);
    }

    /**
     * @method: 判断桶是否存在
     * @param bucketName:
     * @return: boolean
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public boolean bucketExists(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
    }

    /**
     * @method: 创建桶
     * @param bucketName:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public void makeBucket(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
    }

    /**
     * @method: 检查桶是否存在，若不存在则创建
     * @param bucketName:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public void checkBucket(String bucketName) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InsufficientDataException, InternalException {
        boolean bucketExists = bucketExists(bucketName);
        if(!bucketExists) {
            makeBucket(bucketName);
        }
    }

    //桶策略，建议从管理页面进行配置

    /*public String getBucketPolicy(String bucketName) {
        return "";
    }*/

    /*public void setBucketPolicy(String bucketName, String policy) {

    }*/

    /**
     * @method: 删除文件
     * @param buckerName:
     * @param objectName:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public void removerObject(String buckerName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        minioClient.removeObject(RemoveObjectArgs.builder()
                .bucket(buckerName)
                .object(objectName)
                .build());
    }

    /**
     * @method: 批量删除文件
     * @param buckerName:
     * @param objectNames:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public void removeObjects(String buckerName, List<String> objectNames) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        List<DeleteObject> objects = new LinkedList<>();
        objectNames.stream().forEach(objectName -> objects.add(new DeleteObject(objectName)));
        Iterable<Result<DeleteError>> results = minioClient.removeObjects(
                RemoveObjectsArgs.builder()
                        .bucket(buckerName)
                        .objects(objects)
                        .build());
        for (Result<DeleteError> result : results) {
            DeleteError error = result.get();
            System.out.println("Error in deleting object " + error.objectName() + "; " + error.message());
        }
    }

    /**
     * @method: 下载文件（新文件为fileName）
     * @param bucketName:
     * @param objectName:
     * @param fileName:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public void downloadObject(String bucketName, String objectName, String fileName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .filename(fileName)
                .build());
    }

    /**
     * @method: 获取文件流
     * @param bucketName:
     * @param objectName:
     * @return: void
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public InputStream getObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.getObject(GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build());
    }

    /**
     * @method: 获取文件信息
     * @param bucketName:
     * @param objectName:
     * @return: java.lang.String
     * @author: namei
     * @date: 2021/3/29
     * @description:
     */
    public String statObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        StatObjectResponse statObjectResponse = minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build());
        return statObjectResponse.toString();
    }

    /**
     * @method: 列出全部桶信息
     * @return: void
     * @author: weiwenbin
     * @date: 2021/3/29 22:32
     * @description:
     */
    public String listBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        List<Bucket> bucketList = minioClient.listBuckets();

        List<Map<String, Object>> resultList = new ArrayList<>();

        bucketList.stream().forEach(bucket -> {
            Map<String, Object> map = new HashMap<>();
            map.put("name", bucket.name());
            map.put("creationDate", bucket.creationDate());
            resultList.add(map);
        });

        return new ObjectMapper().writeValueAsString(resultList);
    }

    /** 列出桶的文件信息
     * @method:
     * @param bucketName:
     * @return: void
     * @author: weiwenbin
     * @date: 2021/3/29 22:36
     * @description:
     */
    public String listObjects(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucketName)
                .recursive(true)
                .build());

        /*Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucketName)
                .build());*/

        for (Result<Item> result : results) {
            Item item = result.get();
            System.out.println(item.lastModified() + ", " + item.size() + ", " + item.objectName());
        }
        return "";
    }
}
