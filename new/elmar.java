/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package sun.javaclients.kinesisconsumertest1;


import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

/**
*
* @author elmarm
*/

public class KinConTest1 {
    public static final String APPLICATION_STREAM_NAME = "cor-kinesis-saas1-stg-stu-cm-stellenbosch";

    private static final String APPLICATION_NAME = "cor-kinesis-saas1-stg-stu-cm-stellenbosch";

    private static final String APPLICATION_REGION = "eu-west-1";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

    private static AWSCredentialsProvider credentialsProvider;

        private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
    }
    /**
     * @param args the command line arguments
    */
    public static void main(String[] args) throws Exception {
        init();
        //
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(APPLICATION_NAME,
                        APPLICATION_STREAM_NAME,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
        kinesisClientLibConfiguration.withRegionName(APPLICATION_REGION);
        IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
        System.out.printf("Running %s to process stream %s as worker %s...\n",
                APPLICATION_NAME,
                APPLICATION_STREAM_NAME,
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

}
