package com.webflux.s3.controller;

import com.webflux.s3.service.S3FileUploadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("file")
public class S3FileUploadController {

    @Autowired
    private S3FileUploadService s3FileUploadService;

    // use single FilePart for single file upload
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(value = HttpStatus.OK)
    public Mono<List<String>> upload(@RequestPart("file") FilePart filePart) {

        /*
          To see the response beautifully we are returning strings as Mono List
          of String. We could have returned Flux<String> from here.
          If you are curious enough then just return Flux<String> from here and
          see the response on Postman
         */
        return s3FileUploadService.getLines(filePart).collectList();
    }

    private final Path basePath = Paths.get("./src/main/resources/");

    @PostMapping(value = "/uploadFile", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<String> uploadFile(@RequestPart("fileToUpload") Mono<FilePart> filePartMono) throws IOException {
        final String[] fileName = new String[1];
        filePartMono.map(filePart -> {
            return fileName[0] = filePart.filename();
        });
        createFile(filePartMono);
        InputStream is = new FileInputStream(basePath.toString()+ fileName[0]);

        String bucketName = "myjavabucket12345";
        String objectKey = fileName[0];
        String objectPath = basePath+ fileName[0];
        System.out.println("Putting object " + objectKey +" into bucket "+bucketName);
        System.out.println("  in bucket: " + bucketName);

        S3Client s3Client = S3Client.builder().build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("author", "Gaurav Amritkar");
        metadata.put("version", "1.0.0.1");

        PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .metadata(metadata)
                .build();

        PutObjectResponse response = s3Client.putObject(putOb, RequestBody.fromInputStream(is, is.available()));
        //new File(basePath+fileName).delete();
        return  Mono.just(response.eTag());
    }

    private Mono<Void> createFile(Mono<FilePart> filePartMono){
        return  filePartMono
                .doOnNext(fp -> System.out.println("Received File : " + fp.filename()))
                .flatMap(fp -> fp.transferTo(basePath.resolve(fp.filename())))
                .then();
    }
}
