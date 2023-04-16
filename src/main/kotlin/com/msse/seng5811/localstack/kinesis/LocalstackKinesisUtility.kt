package com.msse.seng5811.localstack.kinesis

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.CreateStreamRequest
import com.amazonaws.services.kinesis.model.DeleteStreamRequest
import com.msse.seng5811.localstack.LocalstackUtility

object LocalstackKinesisUtility {
    fun getKinesisClient(): AmazonKinesis = buildKinesisClient(
        endpoint = LocalstackUtility.getEndpoint(),
        region = LocalstackUtility.region,
        awsCredentials = LocalstackUtility.awsCredentials
    )

    fun refreshKinesisStream(streamName: String, shardCount: Int = 1) {
        val kinesisClient = getKinesisClient()
        if (kinesisClient.listStreams().streamNames.contains(streamName)) deleteStream(streamName)
        Thread.sleep(500)
        createStream(streamName, shardCount)
        Thread.sleep(500)
    }

    private fun buildKinesisClient(endpoint: String, region: String, awsCredentials: BasicAWSCredentials): AmazonKinesis =
        AmazonKinesisClientBuilder.standard()
            .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(awsCredentials.awsAccessKeyId, awsCredentials.awsSecretKey)))
            .withEndpointConfiguration(LocalstackUtility.localStackEndpointConfiguration(endpoint, region))
            .build()

    private fun createStream(name: String, shardCount: Int = 1) {
        val kinesisClient = getKinesisClient()
        val createRequest = CreateStreamRequest().withStreamName(name).withShardCount(shardCount)
        kinesisClient.createStream(createRequest)
    }

    private fun deleteStream(name: String) {
        val kinesisClient = getKinesisClient()
        val deleteRequest = DeleteStreamRequest().withStreamName(name)
        kinesisClient.deleteStream(deleteRequest)
    }
}