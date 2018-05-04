package co.uk.bbc.test.lambda;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mindrot.jbcrypt.BCrypt;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class Main implements
        RequestHandler<S3Event, String> {


    public String handleRequest(S3Event s3Event, Context context) {
        try {
            S3EventNotificationRecord record = s3Event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();

            //srcBucket should be abro-test-bucket. Lets check it!

            if(!srcBucket.equals("abro-test-bucket")) {
                System.err.println("wrong source bucket name. Expected abro-test-bucket, but was "
                        + srcBucket);
                return "";
            }

            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');

            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            String dstBucket = "abro-test-output";


            String dstKey = "output-" + srcKey;

            // Sanity check: validate that source and destination are different
            // buckets.
            if (srcBucket.equals(dstBucket)) {
                System.out
                        .println("Destination bucket must not match source bucket.");
                return "";
            }

            // TODO: Check file format and content

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            InputStream objectData = s3Object.getObjectContent();

            List<String> output = new ArrayList<>();

            try(InputStreamReader reader = new InputStreamReader(objectData);
                CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT)            ) {
                for (CSVRecord r : parser) {
                    String outputRecord = r.get(0) + "," + BCrypt.hashpw(r.get(1), BCrypt.gensalt());
                    output.add(outputRecord);
                }
            }

            // write output
            ByteArrayOutputStream os = new ByteArrayOutputStream();

            // TODO: More then one entry!!
            os.write(output.get(0).getBytes(Charset.forName("UTF-8")));


            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(os.size());

            System.out.println("Writing to" + dstBucket + "/" + dstKey);
            s3Client.putObject(dstBucket, dstKey, new ByteArrayInputStream(os.toByteArray()), meta);
            System.out.println("Successfully output to bucket");

            return "Ok";



        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
