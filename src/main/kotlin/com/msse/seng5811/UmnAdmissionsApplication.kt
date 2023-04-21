package com.msse.seng5811

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.*
import com.amazonaws.services.s3.AmazonS3
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import com.msse.seng5811.localstack.kinesis.LocalKinesisStreams
import com.msse.seng5811.localstack.kinesis.LocalstackKinesisUtility
import com.msse.seng5811.utils.ObjectMapper
import com.msse.seng5811.localstack.s3.LocalS3Buckets
import com.msse.seng5811.localstack.s3.LocalstackS3Utility
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

/**
 * A simple application that converts eligible [UmnApplicant] objects into [UmnStudent] object.
 */
object UmnAdmissionsApplication {
    private val mapper = ObjectMapper.mapper
    private val log: Logger = LoggerFactory.getLogger(UmnAdmissionsApplication::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        // Start the application locally with the help of Localstack mocked resource name and AWS clients.
        start(
            inputStreamName = LocalKinesisStreams.INPUT_STREAM,
            outputBucketName = LocalS3Buckets.OUTPUT_BUCKET,
            kinesis = LocalstackKinesisUtility.getKinesisClient(),
            s3 = LocalstackS3Utility.getS3Client()
        )
    }

    /**
     * Starts a long-running processing loop that continuously reads data that continuously reads a list [UmnApplicant]
     * records from a specific kinesis stream and converts all eligible applicants to [UmnStudent] records and places
     * them into a specific S3 bucket.
     */
    fun start(inputStreamName: String, outputBucketName: String, kinesis: AmazonKinesis, s3: AmazonS3) {
        log.info("University of Minnesota is now accepting applicants!")
        log.info("Waiting for applicants on kinesis $inputStreamName...")

        var shardIterator = getShardIterator(inputStreamName, kinesis)
        while (shardIterator != null) {
            // Get Records from shard
            val getRecordsResult = try {
                val getRecordsRequest = GetRecordsRequest().withShardIterator(shardIterator)
                kinesis.getRecords(getRecordsRequest)
            } catch (e: Exception) {
                handleException(e)
                continue
            }

            // If no records are present proceed to the next shard.
            if (getRecordsResult.records.isEmpty()) {
                shardIterator = getRecordsResult.nextShardIterator
                continue
            }

            // Parse kinesis input records to UmnApplicant objects
            val umnApplicants: List<UmnApplicant> = getRecordsResult.records.map {
                mapper.readValue(it.data.array(), UmnApplicant::class.java)
            }

            log.info("Received ${umnApplicants.size} umn applicants...")

            // Convert UmnApplicants with gpa > 3.0 to UmnStudents and publish to s3
            umnApplicants
                .filter { it.gpa >= 3.0 }
                .also { log.info("Admitted ${it.size} / ${umnApplicants.size} applicants to University of Minnesota!") }
                .forEach {
                    publishObjectToS3Bucket(
                        bucketName = outputBucketName,
                        s3 = s3,
                        umnStudent = UmnStudent(
                            umnApplicant = it,
                            umnId = UUID.randomUUID().toString(),
                            admissionTimeStamp = Instant.now(),
                        )
                    )
                }

            log.info("Finished Processing ${umnApplicants.size} umn applicants!")
            shardIterator = getRecordsResult.nextShardIterator
        }
    }

    /**
     * Given a stream name, retrieves a [GetShardIteratorResult] for the first shard of the kinesis stream
     * using the applications kinesis client.
     */
    private fun getShardIterator(streamName: String, kinesis: AmazonKinesis): String? {
        // Get all shards of the kinesis stream
        val listShardsRequest = ListShardsRequest().withStreamName(streamName)
        val listShardResult = kinesis.listShards(listShardsRequest)

        // Create a shard iterator request for the first shard (assumes kinesis stream only has one shard).
        val getShardIteratorRequest = GetShardIteratorRequest()
            .withStreamName(streamName)
            .withShardId(listShardResult.shards.first().shardId)
            .withShardIteratorType(ShardIteratorType.LATEST)

        // Return Shard Iterator for the kinesis shard
        return kinesis.getShardIterator(getShardIteratorRequest).shardIterator
    }

    /**
     * Given a s3 bucket name, [AmazonS3] client and [UmnStudent] object publishes object to the specified bucket.
     */
    private fun publishObjectToS3Bucket(bucketName: String, s3: AmazonS3, umnStudent: UmnStudent) =
        s3.putObject(
            bucketName, // Bucket Name
            umnStudent.umnId, // Object Key
            mapper.writeValueAsString(umnStudent) // Payload
        )

    private fun handleException(e: Exception) =
        when (e) {
            is ResourceNotFoundException -> log.info("ShardId not found in stream!. Exception: $e")
            is ProvisionedThroughputExceededException -> {
                log.info("GetRecords ProvisionedThroughputExceededException. Delaying getRecords calls by 5s.")
                runBlocking { delay(5000) }
            }
            is ResourceInUseException -> println("Resource Still in Use, try again.")
            // Silence the AbortedException for when System Tests kill the thread after execution.
            is AbortedException -> Unit // Do nothing
            else -> log.info("Unexpected Amazon Kinesis Exception: $e")
        }
}