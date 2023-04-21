package com.msse.seng5811

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.util.IOUtils
import com.msse.seng5811.localstack.kinesis.LocalKinesisStreams
import com.msse.seng5811.localstack.kinesis.LocalstackKinesisUtility
import com.msse.seng5811.utils.ObjectMapper
import com.msse.seng5811.localstack.s3.LocalS3Buckets
import com.msse.seng5811.localstack.s3.LocalstackS3Utility
import com.msse.seng5811.utils.within
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.time.Duration
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UmnAdmissionsApplicationSystemTest {
    private val kinesis = LocalstackKinesisUtility.getKinesisClient()
    private val s3 = LocalstackS3Utility.getS3Client()
    private val testInputStream = LocalKinesisStreams.INPUT_STREAM
    private val testOutputBucket = LocalS3Buckets.OUTPUT_BUCKET
    private var daemonThread: Thread = Thread()

    companion object {
        val log: Logger = LoggerFactory.getLogger(UmnAdmissionsApplicationSystemTest::class.java)
    }

    @BeforeEach
    @AfterAll
    fun sanitize() {
        // Kill the application after execution of each test.
        try { daemonThread.interrupt() } catch (e: AbortedException) { log.info("End of test.") }

        // Cleanup
        LocalstackS3Utility.refreshS3Bucket(testOutputBucket)
        LocalstackKinesisUtility.refreshKinesisStream(testInputStream)
        log.info("Cleaned up test objects!")
    }

    @Test
    fun `umnAdmissionsApplication -- mix of eligible and ineligible applicants -- only eligible applicants are admitted`() {
        // SETUP

        log.info("Starting UmnAdmissionsApplications System Test")
        val eligibleApplicants = createUmnApplicants(count = Random.nextInt(0, 50), eligible = true)
        val ineligibleApplicants = createUmnApplicants(count = Random.nextInt(0, 50), eligible = false)
        val allApplicants: List<UmnApplicant> = eligibleApplicants + ineligibleApplicants

        // Start application under test.
        startApplication(
            inputStreamName = testInputStream,
            outputBucketName = testOutputBucket,
            kinesis = kinesis,
            s3 = s3
        )

        // Publish applicants to kinesis
        val putRecordsRequest = createPutRecordsRequest<UmnApplicant>(allApplicants.shuffled())
        kinesis.putRecords(putRecordsRequest)

        // Wait for all records to publish successfully
        Thread.sleep(1000)

        // Verify we get expected results within 5 seconds
        within(Duration.ofSeconds(5)) {
            // Retrieve output from S3.
            val umnStudents: List<UmnStudent> = retrieveObjectsFromS3Bucket<UmnStudent>(s3, testOutputBucket)

            // Assert only and all eligible candidates are converted into UMN students
            assertThat(umnStudents.size).isEqualTo(eligibleApplicants.size)
            assertThat(umnStudents.map { it.umnApplicant })
                .containsOnlyOnceElementsOf(eligibleApplicants)
                .doesNotContainAnyElementsOf(ineligibleApplicants)

            // Assert UMN IDs for each student is unique.
            assertThat(umnStudents.map { it.umnId }.distinct().size).isEqualTo(eligibleApplicants.size)
            log.info("UmnAdmissions System Test Passed!")
        }
    }

    /**
     * Starts [UmnAdmissionsApplication] in a separate thread.
     */
    private fun startApplication(
        inputStreamName: String,
        outputBucketName: String,
        kinesis: AmazonKinesis,
        s3: AmazonS3
    ) {
        daemonThread = Thread {
            UmnAdmissionsApplication.start(
                inputStreamName = inputStreamName,
                outputBucketName = outputBucketName,
                kinesis = kinesis,
                s3 = s3
            )
        }.also {
            it.isDaemon = true
            it.start()
        }
    }

    /**
     * Retrieves [String] objects from a specific s3 bucket using a [AmazonS3] client and deserializes them to
     * the supplied reified type [T].
     */
    private inline fun <reified T> retrieveObjectsFromS3Bucket(s3: AmazonS3, bucketName: String): List<T> {
        val listObjectsResult = s3.listObjects(ListObjectsRequest().withBucketName(bucketName))
        val keys = listObjectsResult.objectSummaries.map { it.key }
        val s3Objects = keys.map { s3.getObject(bucketName, it) }
        val s3ObjectContents = s3Objects.map { it.objectContent }
        return s3ObjectContents.map { ObjectMapper.mapper.readValue(IOUtils.toString(it), T::class.java) }
    }

    /**
     * Creates Test [UmnApplicant] objects.
     */
    private fun createUmnApplicants(count: Int, eligible: Boolean): List<UmnApplicant> {
        val umnApplicants = mutableListOf<UmnApplicant>()
        repeat(count) {
            umnApplicants.add(
                UmnApplicant(
                    name = "AcceptedApplicant$it",
                    age = Random.nextInt(1, 100),
                    gpa = when {
                        eligible -> Random.nextDouble(3.0, 4.0)
                        else -> Random.nextDouble(2.0, 3.0)
                    }
                )
            )
        }
        return umnApplicants
    }

    /**
     * Creates a [PutRecordsRequest] for a list of [T] objects to kinesis by serializing them and using the
     * serialization time as the partition key for kinesis transmission.
     */
    @OptIn(ExperimentalTime::class)
    private inline fun <reified T> createPutRecordsRequest(objects: List<T>): PutRecordsRequest {
        val putRecordsRequestEntries = mutableListOf<PutRecordsRequestEntry>()
        objects.forEach {
            val (serialized, serializationTime) = measureTimedValue { ObjectMapper.mapper.writeValueAsString(it) }
            putRecordsRequestEntries += PutRecordsRequestEntry()
                .withData(ByteBuffer.wrap(serialized.toByteArray(Charsets.UTF_8)))
                .withPartitionKey(serializationTime.inWholeMilliseconds.toString())
        }

        return PutRecordsRequest()
            .withRecords(putRecordsRequestEntries)
            .withStreamName(testInputStream)
    }
}