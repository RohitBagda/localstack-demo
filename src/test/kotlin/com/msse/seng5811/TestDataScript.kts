import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.msse.seng5811.UmnApplicant
import com.msse.seng5811.localstack.kinesis.LocalKinesisStreams
import com.msse.seng5811.localstack.kinesis.LocalstackKinesisUtility
import com.msse.seng5811.utils.ObjectMapper
import java.nio.ByteBuffer
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

val kinesis = LocalstackKinesisUtility.getKinesisClient()
val testInputStream = LocalKinesisStreams.INPUT_STREAM

val eligibleApplicants: List<UmnApplicant> = createUmnApplicants(count = Random.nextInt(0, 50), eligible = true)
val ineligibleApplicants: List<UmnApplicant> = createUmnApplicants(count = Random.nextInt(0, 50), eligible = false)
val allApplicants: List<UmnApplicant> = eligibleApplicants + ineligibleApplicants

// Publish applicants to kinesis
val putRecordsRequest = createPutRecordsRequest<UmnApplicant>(allApplicants.shuffled())
kinesis.putRecords(putRecordsRequest)

println("Successfully published ${allApplicants.size} applicants to kinesis.")
println("Eligible Applicants: ${eligibleApplicants.size}, Ineligible Applicants: ${ineligibleApplicants.size}")

/**
 * Creates Test [UmnApplicant] objects.
 */
fun createUmnApplicants(count: Int, eligible: Boolean): List<UmnApplicant> {
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
inline fun <reified T> createPutRecordsRequest(objects: List<T>): PutRecordsRequest {
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