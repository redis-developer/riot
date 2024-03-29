package com.redis.riot.cli;

import java.io.File;

import com.redis.riot.file.AmazonS3Options;
import com.redis.riot.file.FileOptions;
import com.redis.riot.file.GoogleStorageOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileArgs {

    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
    String encoding = FileOptions.DEFAULT_ENCODING;

    @Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
    boolean gzipped;

    @ArgGroup(exclusive = false)
    S3Args s3 = new S3Args();

    @ArgGroup(exclusive = false)
    GcsArgs gcs = new GcsArgs();

    public FileOptions fileOptions() {
        FileOptions options = new FileOptions();
        options.setAmazonS3Options(s3.amazonS3Options());
        options.setEncoding(encoding);
        options.setGoogleStorageOptions(gcs.googleStorageOptions());
        options.setGzipped(gzipped);
        return options;
    }

    private static class S3Args {

        @Option(names = "--s3-access", description = "Access key.", paramLabel = "<key>")
        private String accessKey;

        @Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key.", paramLabel = "<key>")
        private String secretKey;

        @Option(names = "--s3-region", description = "AWS region.", paramLabel = "<name>")
        private String region;

        public AmazonS3Options amazonS3Options() {
            AmazonS3Options options = new AmazonS3Options();
            options.setAccessKey(accessKey);
            options.setSecretKey(secretKey);
            options.setRegion(region);
            return options;
        }

    }

    private static class GcsArgs {

        @Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json).", paramLabel = "<file>")
        private File keyFile;

        @Option(names = "--gcs-project", description = "GCP project id.", paramLabel = "<id>")
        private String projectId;

        @Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key.", paramLabel = "<key>")
        private String encodedKey;

        public GoogleStorageOptions googleStorageOptions() {
            GoogleStorageOptions options = new GoogleStorageOptions();
            options.setKeyFile(keyFile);
            options.setProjectId(projectId);
            options.setEncodedKey(encodedKey);
            return options;
        }

    }

}
