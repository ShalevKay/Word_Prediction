package operations;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class S3Operations {

    private final S3Client s3Client;

    /**
     * Constructor: Initialize the S3 client with region.
     *
     * @param region    AWS Region (e.g., "us-east-1")
     */
    public S3Operations(String region) {
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .build();
    }

    /**
     * Uploads a file to the specified S3 bucket.
     *
     * @param bucketName Target S3 bucket name
     * @param key        Object key (path in the bucket)
     * @param filePath   Local file path to upload
     */
    public void uploadFile(String bucketName, String key, String filePath) {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.putObject(putRequest, Paths.get(filePath));
        System.out.println("File uploaded successfully: " + key);
    }

    /**
     * Downloads a file from the specified S3 bucket.
     *
     * @param bucketName Source S3 bucket name
     * @param key        Object key (path in the bucket)
     * @param downloadPath Local path to save the downloaded file
     */
    public void downloadFile(String bucketName, String key, String downloadPath) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.getObject(getRequest, Paths.get(downloadPath));
        System.out.println("File downloaded successfully: " + downloadPath);
    }

    /**
     * Lists all objects in the specified S3 bucket.
     *
     * @param bucketName S3 bucket name
     * @return List of object keys in the bucket
     */
    public List<String> listObjects(String bucketName) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        List<String> keys = listResponse.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());

        System.out.println("Objects in bucket: " + bucketName);
        keys.forEach(System.out::println);
        return keys;
    }

    /**
     * Deletes an object from the specified S3 bucket.
     *
     * @param bucketName Target S3 bucket name
     * @param key        Object key (path in the bucket) to delete
     */
    public void deleteFile(String bucketName, String key) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.deleteObject(deleteRequest);
        System.out.println("File deleted successfully: " + key);
    }

    /**
     * Closes the S3 client.
     */
    public void close() {
        s3Client.close();
    }
}
