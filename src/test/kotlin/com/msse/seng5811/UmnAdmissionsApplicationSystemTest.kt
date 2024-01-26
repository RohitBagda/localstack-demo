package com.msse.seng5811

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.*
import com.msse.seng5811.localstack.kinesis.LocalKinesisStreams
import com.msse.seng5811.localstack.kinesis.LocalstackKinesisUtility
import com.msse.seng5811.utils.ObjectMapper
import com.msse.seng5811.utils.within
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
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
    private val testInputStream = LocalKinesisStreams.INPUT_STREAM
    private val testOutputStream = LocalKinesisStreams.OUTPUT_STREAM
    private var daemonThread: Thread = Thread()
    private val mapper = ObjectMapper.mapper


    companion object {
        val log: Logger = LoggerFactory.getLogger(UmnAdmissionsApplicationSystemTest::class.java)
    }

    @BeforeEach
    @AfterAll
    fun sanitize() {
        // Kill the application after execution of each test.
        try { daemonThread.interrupt() } catch (e: AbortedException) { log.info("End of test.") }

        // Cleanup
        LocalstackKinesisUtility.refreshKinesisStream(testInputStream)
        LocalstackKinesisUtility.refreshKinesisStream(testOutputStream)
        log.info("Cleaned up test objects!")
    }

    @Test
    fun `umnAdmissionsApplication -- mix of eligible and ineligible applicants -- only eligible applicants are admitted`() {
        // SETUP
        log.info("Starting UmnAdmissionsApplications System Test")
        val eligibleApplicants = createUmnApplicants(count = Random.nextInt(0, 50), eligible = true)
        val ineligibleApplicants = createUmnApplicants(count = Random.nextInt(0, 50), eligible = false)
        val allApplicants: List<UmnApplicant> = eligibleApplicants + ineligibleApplicants
        log.info("Successfully published ${allApplicants.size} applicants to kinesis.")
        log.info("Eligible Applicants: ${eligibleApplicants.size}, Ineligible Applicants: ${ineligibleApplicants.size}")

        // Start application under test.
        startApplication(
            inputStreamName = testInputStream,
            outputStreamName = testOutputStream,
            kinesis = kinesis,
        )

        // Publish applicants to kinesis
        val putRecordsRequest = createPutRecordsRequest<UmnApplicant>(allApplicants.shuffled())
        kinesis.putRecords(putRecordsRequest)
        Thread.sleep(5000)

        // Verify we get expected results within 5 seconds
        within(Duration.ofSeconds(5)) {
            // Retrieve output from Kinesis
            val umnStudents: List<UmnStudent> = readObjectsFromKinesis<UmnStudent>()

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
        outputStreamName: String,
        kinesis: AmazonKinesis,
    ) {
        daemonThread = Thread {
            UmnAdmissionsApplication.start(
                inputStreamName = inputStreamName,
                outputStreamName = outputStreamName,
                kinesis = kinesis
            )
        }.also {
            it.isDaemon = true
            it.start()
        }
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

    private inline fun <reified T> readObjectsFromKinesis(): List<UmnStudent> {
        val students = mutableListOf<UmnStudent>()
        // Get Records from shard
        val shardIterator = LocalstackKinesisUtility.getShardIterator(testOutputStream, kinesis)
        val getRecordsResult = try {
            val getRecordsRequest = GetRecordsRequest().withShardIterator(shardIterator)
            kinesis.getRecords(getRecordsRequest)
        } catch (e: Exception) {
            handleException(e)
            return emptyList()
        }

        // If no records are present proceed to the next shard iterator.
        if (getRecordsResult.records.isEmpty()) {
            return emptyList()
        }

        // Parse kinesis input records to UmnApplicant objects
        getRecordsResult.records.map {
            students += mapper.readValue(it.data.array(), UmnStudent::class.java)
        }

        return students
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