package edu.uw.kualico.consumer.thread.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

public class KcmRecordRequester {

    private final static Logger logger = LoggerFactory.getLogger(KcmRecordRequester.class);

    public final static Long DEFAULT_SHARD_ITERATOR_TIMEOUT = 60000l;

    public final static Integer DEFAULT_RECORD_RECEIVE_LIMIT = 50;

    protected String shardId;

    protected String streamName;

    /**
     * When a shard iterator is requested from Kinesis, it will expire in 5 minutes.
     *
     * This timeout needs to be less that 5 minutes, and will let the code know when to request a new iterator.
     */
    protected Long shardIteratorTimeout = DEFAULT_SHARD_ITERATOR_TIMEOUT;

    protected Long lastShardIteratorRequestMillis;

    protected Integer recordReceiveLimit = DEFAULT_RECORD_RECEIVE_LIMIT;

    protected AmazonKinesisClient kinesisClient;

    protected GetShardIteratorRequest shardIteratorRequest;

    protected String currentShardIterator;

    public List<Record> getRecords(BigInteger lastSequenceNumber) {

        String shardIterator = currentShardIterator(lastSequenceNumber);

        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setLimit(getRecordReceiveLimit());
        recordsRequest.setShardIterator(shardIterator);

        GetRecordsResult result = getKinesisClient().getRecords(recordsRequest);

        // if the records request has 0 records, and a different shard iterator is given
        // then request records from the new shard iterator
        if (result.getRecords().isEmpty()) {
            String nextShardIterator = result.getNextShardIterator();
            recordsRequest = new GetRecordsRequest();
            recordsRequest.setLimit(getRecordReceiveLimit());
            recordsRequest.setShardIterator(nextShardIterator);

            result = getKinesisClient().getRecords(recordsRequest);
        }

        logger.debug("Record request successful, returned: " + result.getRecords().size() + " records");

        return result.getRecords();
    }

    /**
     * Return the current shard iterator, which is an identifier string set on data record requests to Kinesis.
     *
     * If a shard iterator has not been requested, or the timeout has elapsed,
     * a new shard iterator request will be made through the kinesis client
     *
     * @param lastSequenceNumber the last successfully processed sequence number in the retrieved records
     *
     * @return the shard iterator
     */
    protected String currentShardIterator(BigInteger lastSequenceNumber) {

        String requestedShardIterator = null;
        if( shardIteratorRequest == null ||  System.currentTimeMillis() > lastShardIteratorRequestMillis + getShardIteratorTimeout() ) {

            if (lastSequenceNumber != null && lastSequenceNumber.compareTo(new BigInteger("0")) != 0) {
                logger.info("New shard iterator requested, starting at sequence: " + lastSequenceNumber);

                // if have not requested a shard iterator, or the timeout has passed since the last shard iterator request
                // then request a new shard iterator
                shardIteratorRequest = new GetShardIteratorRequest();
                shardIteratorRequest.setStreamName(getStreamName());
                shardIteratorRequest.setShardId(getShardId());
                shardIteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
                shardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber.toString());


            }
            else {
                // if no sequence number is set, start a shard iterator at the latest message
                logger.info("New shard iterator requested, last sequence number is null or 0, starting at latest message");

                // if have not requested a shard iterator, or the timeout has passed since the last shard iterator request
                // then request a new shard iterator
                shardIteratorRequest = new GetShardIteratorRequest();
                shardIteratorRequest.setStreamName(getStreamName());
                shardIteratorRequest.setShardId(getShardId());
                shardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST);
            }

            GetShardIteratorResult shardIteratorResult = getKinesisClient().getShardIterator(shardIteratorRequest);

            this.lastShardIteratorRequestMillis = System.currentTimeMillis();
            requestedShardIterator = shardIteratorResult.getShardIterator();
        }

        String shardIterator;
        boolean usingRequestedIterator = false;
        if (StringUtils.isBlank(requestedShardIterator)) {
            shardIterator = this.currentShardIterator;
            logger.debug("No requested shard iterator, using existing current value: " + this.currentShardIterator);
        }
        else {
            shardIterator = requestedShardIterator;
            logger.debug("Using requested shard iterator: " + requestedShardIterator);
            usingRequestedIterator = true;
        }

        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setLimit(getRecordReceiveLimit());
        recordsRequest.setShardIterator(shardIterator);

        GetRecordsResult result = getKinesisClient().getRecords(recordsRequest);

        logger.debug("Record request test returned: " + result.getRecords().size() + " records, next shard iterator = " + result.getNextShardIterator());

        // if the requested iterator is used, it may be many shards behind from the next actual message
        // So the current shard iterator is updated only when there are records found in the shard from the retrieved iterator
        if(usingRequestedIterator) {
            if (!result.getRecords().isEmpty()) {
                logger.debug("A request was made for a new shard iterator, and that iterator contained records");
                logger.debug("Current iterator set to requested iterator");
                this.currentShardIterator = requestedShardIterator;
            }
            else {
                logger.debug("A request was made for a new shard iterator, and that iterator did not contain records");
                // if the current shard iterator is null, set it equal to the next shard from the empty request
                // this happens when handling the first retrieve request and the first shard iterator has 0 records
                if (StringUtils.isBlank(this.currentShardIterator)) {
                    logger.debug("Current iterator had no prior value, set to next iterator from record request");
                    this.currentShardIterator = result.getNextShardIterator();
                }
                else {
                    // if there are records in the shard, see if any have a sequence number larger than the requested one
                    boolean foundLargerSequence = false;
                    for (Record record : result.getRecords()) {
                        if (StringUtils.isNotBlank(record.getSequenceNumber())) {
                            BigInteger recordSequenceNum = new BigInteger(record.getSequenceNumber());

                            if (recordSequenceNum.compareTo(lastSequenceNumber) > 0) {
                                foundLargerSequence = true;
                                break;
                            }
                        }
                    }


                    if (foundLargerSequence) {
                        // if a larger sequence was found, the requested shard iterator has new records
                        // so the currentShardIterator can be set to the requested one
                        logger.debug("Record with larger sequence found in shard of requested iterator");
                        logger.debug("Current iterator set to requested iterator");
                        this.currentShardIterator = requestedShardIterator;
                    }
                    else {
                        // otherwise, set the current shard iterator to the next value from the record request
                        logger.debug("No records with larger sequence found in shard of requested iterator");
                        logger.debug("Current iterator set to next iterator from record request");
                        this.currentShardIterator = result.getNextShardIterator();
                    }
                }
            }
        }
        else {
            logger.debug("No new request was made for a new iterator");
            // if the records request has 0 records, and a different shard iterator is given
            // then request records from the new shard iterator
            if (result.getRecords().isEmpty()) {
                this.currentShardIterator = result.getNextShardIterator();
                logger.debug("A request for records returned empty");
                logger.debug("Current iterator set to next iterator from record request");
            } else {
                logger.debug("A request for records returned records");
                logger.debug("Current iterator set to the iterator used to make the record request");
                this.currentShardIterator = shardIterator;
            }
        }

        return currentShardIterator;

    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public Long getShardIteratorTimeout() {
        return shardIteratorTimeout;
    }

    // This property has a default value, so the setter is not used in most configurations
    @SuppressWarnings("unused")
    public void setShardIteratorTimeout(Long shardIteratorTimeout) {
        this.shardIteratorTimeout = shardIteratorTimeout;
    }

    public Integer getRecordReceiveLimit() {
        return recordReceiveLimit;
    }

    // This property has a default value, so the setter is not used in most configurations
    @SuppressWarnings("unused")
    public void setRecordReceiveLimit(Integer recordReceiveLimit) {
        this.recordReceiveLimit = recordReceiveLimit;
    }

    public AmazonKinesisClient getKinesisClient() {
        return kinesisClient;
    }

    public void setKinesisClient(AmazonKinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

}
