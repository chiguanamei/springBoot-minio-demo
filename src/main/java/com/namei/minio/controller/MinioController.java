package com.namei.minio.controller;

import com.namei.minio.util.MinioUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * @author: namei
 * @date: 2021/3/29
 * @description:
 */
@RestController
@RequestMapping("/minio")
public class MinioController {

    @Autowired
    private MinioUtil minioUtil;

    @PostMapping("putObject")
    public String putObject(MultipartFile file,
                         @RequestParam(name = "bucketName") String bucketName,
                         @RequestParam(name = "fileName", defaultValue = "") String fileName){
        try {
            String url = minioUtil.putObject(
                    bucketName,
                    StringUtils.isBlank(fileName) ? file.getOriginalFilename() : fileName,
                    file.getInputStream(),
                    file.getContentType()
            );
            return url;
        } catch (Exception e){
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @PostMapping("uploadObject")
    public String uploadObject(
                            @RequestParam(name = "bucketName") String bucketName,
                            @RequestParam(name = "fileName") String fileName){
        try {
            String url = minioUtil.uploadObject(
                    bucketName,
                    fileName
            );
            return url;
        } catch (Exception e){
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @PostMapping("removerObjects")
    public String removerObjects(@RequestBody Map<String, Object> map) {
        try {
            minioUtil.removeObjects((String) map.get("bucketName"), (List<String>) map.get("objectNames"));
            return "删除成功";
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @PostMapping("downloadObejct")
    public void downloadObject(@RequestBody Map<String, String> map) {
        try {
            minioUtil.downloadObject(map.get("bucketName"), map.get("objectName"), map.get("fileName"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("getObject")
    public void getObject(@RequestBody Map<String, String> map) {
        try (InputStream is = minioUtil.getObject(map.get("bucketName"), map.get("objectName"))){
            FileUtils.copyInputStreamToFile(is, new File(map.get("fileName")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("statObject")
    public String statObject(@RequestBody Map<String, String> map) {
        try {
            return minioUtil.statObject(map.get("bucketName"), map.get("objectName"));
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }


    @PostMapping("listBuckets")
    public String listBuckets() {
        try {
            return minioUtil.listBuckets();
        } catch (Exception e){
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @PostMapping("listObjects")
    public String listObjects(@RequestBody Map<String, String> map) {
        try {
            return minioUtil.listObjects(map.get("bucketName"));
        } catch (Exception e){
            e.printStackTrace();
            return e.getMessage();
        }
    }
}
