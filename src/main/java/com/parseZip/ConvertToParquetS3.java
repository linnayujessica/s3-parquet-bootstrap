package com.parseZip;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ConvertToParquetS3 {

    public static void main(String[] args) {
        ZipToParquet();
    }

    private final static String bucketName = "candidate-71-s3-bucket";
    private final static String accessKeyId = "AKIAZUO64Q7BLCAZZXGV";
    private final static String secretAccessKey = "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";
    private final static String awsRegion = "ap-southeast-2";

    private final static String csvDir = "src/main/resources/unzip";
    private final static String parquetDir = "src/main/resources/parquet";

    public static void ZipToParquet() {
        S3Object s3Object = downloadZipFromAwsS3();
        // unzip
        S3ObjectInputStream stream = s3Object.getObjectContent();
        unzipFiles(stream);
        // read&filter csv and convert to apache parquet
        convertCsvToParquet();

    }

    private static void convertCsvToParquet() {
        try {
            File dir = new File(csvDir);
            for(File csvFile : dir.listFiles()) {
                System.out.println("================== CSV file name: "+csvFile.getPath());
                Path parquetPath = Paths.get(parquetDir, csvFile.getName().replace(".csv", ".parquet"));
                if (Files.exists(parquetPath)) {
                    Files.delete(parquetPath);
                }
                System.out.println("================== Parquet file name: "+parquetPath);
                // define schema
                String firstLine = com.google.common.io.Files.readFirstLine(csvFile, Charset.defaultCharset());
                String[] columns = firstLine.split(",");
                StringBuilder schema = new StringBuilder("message csv {");
                for(int i=0 ; i<columns.length ; i++) {
                    schema.append("required binary column_"+(i+1)+" = "+(i+1)+"; ");
                }
                schema.append("}");
                MessageType messageType = MessageTypeParser.parseMessageType(schema.toString());
                WriteSupport<List<String>> writeSupport = new CsvWriteSupport(messageType);

                String line;
                try (CsvParquetWriter writer = new CsvParquetWriter(new org.apache.hadoop.fs.Path(parquetPath.toUri()), writeSupport);
                     BufferedReader br = new BufferedReader(new FileReader(csvFile.getPath()))) {

                    while((line = br.readLine()) != null) {
                        // filter lines which contain the word "ellipsis" in any fields
                        String[] lines = line.split(",");
                        List list = Arrays.stream(lines).filter(s -> s.contains("ellipsis")).collect(Collectors.toList());
                        if(!list.isEmpty()) {
                            // write specific lines to parquet
                            String[] fields = line.split(",");
                            writer.write(Arrays.asList(fields));
                        }
                    }
                }
                // upload parquet to AWS S3
                uploadParquetToAwsS3(parquetPath.toFile());

                System.out.println("================== End");
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private static void uploadParquetToAwsS3(File file) {
        try {
            AmazonS3 amazonS3 = buildAmazonS3Client();
            PutObjectRequest request = new PutObjectRequest(bucketName, file.getName(), file);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("binary/octet-stream");
            request.setMetadata(metadata);
            amazonS3.putObject(request);
        } catch(AmazonServiceException e) {
            e.printStackTrace();
        }
    }

    private static void unzipFiles(InputStream fis) {
        File dir = new File(csvDir);
        if(!dir.exists()) dir.mkdirs();

        byte[] buffer = new byte[2048];
        try {
            ZipInputStream zis = new ZipInputStream(fis);
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String fileName = zipEntry.getName();
                File unzipFile = new File(csvDir + File.separator + fileName);
                // create directories in zip
                new File(unzipFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(unzipFile);
                int len;
                while((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                zis.closeEntry();
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
            zis.close();
            fis.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private static AmazonS3 buildAmazonS3Client() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

        return AmazonS3ClientBuilder
                .standard().withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();

    }

    private static S3Object downloadZipFromAwsS3() {
        AmazonS3 amamzonS3 = buildAmazonS3Client();
        return amamzonS3.getObject(new GetObjectRequest(bucketName, "data.zip"));
    }

}
