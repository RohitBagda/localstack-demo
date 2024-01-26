package com.msse.seng5811

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import com.msse.seng5811.localstack.kinesis.LocalKinesisStreams
import com.msse.seng5811.localstack.kinesis.LocalstackKinesisUtility
import com.msse.seng5811.utils.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

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
            outputStreamName = LocalKinesisStreams.OUTPUT_STREAM,
            kinesis = LocalstackKinesisUtility.getKinesisClient(),
        )
    }

    /**
     * Starts a long-running processing loop that continuously reads a list [UmnApplicant] records from a specific
     * kinesis stream and converts all eligible applicants to [UmnStudent] records and places them into a specific S3
     * bucket.
     */
    fun start(inputStreamName: String, outputStreamName: String, kinesis: AmazonKinesis) {
        log.info("University of Minnesota is now accepting applicants!")
        log.info("Waiting for applicants on kinesis $inputStreamName...")

        var shardIterator = LocalstackKinesisUtility.getShardIterator(inputStreamName, kinesis)
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

            // Convert UmnApplicants with gpa > 3.0 to UmnStudents and publish to kinesis
            umnApplicants
                .filter { it.gpa >= 3.0 }
                .also { log.info("Admitted ${it.size} / ${umnApplicants.size} applicants to University of Minnesota!") }
                .map {
                    UmnStudent(
                        umnApplicant = it,
                        umnId = UUID.randomUUID().toString(),
                        admissionTimeStamp = Instant.now()
                    )
                }
                .also {
                    val putRecordsRequest = createPutRecordsRequest(it, outputStreamName)
                    kinesis.putRecords(putRecordsRequest)
                }

            log.info("Finished Processing ${umnApplicants.size} umn applicants!")
            shardIterator = getRecordsResult.nextShardIterator
        }
    }

    @OptIn(ExperimentalTime::class)
    private inline fun <reified T> createPutRecordsRequest(objects: List<T>, outputStreamName: String): PutRecordsRequest {
        val putRecordsRequestEntries = mutableListOf<PutRecordsRequestEntry>()
        objects.forEach {
            val (serialized, serializationTime) = measureTimedValue { ObjectMapper.mapper.writeValueAsString(it) }
            putRecordsRequestEntries += PutRecordsRequestEntry()
                .withData(ByteBuffer.wrap(serialized.toByteArray(Charsets.UTF_8)))
                .withPartitionKey(serializationTime.inWholeMilliseconds.toString())
        }

        return PutRecordsRequest()
            .withRecords(putRecordsRequestEntries)
            .withStreamName(outputStreamName)
    }

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