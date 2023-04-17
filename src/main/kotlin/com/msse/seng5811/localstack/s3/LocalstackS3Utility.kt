package com.msse.seng5811.localstack.s3

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CreateBucketRequest
import com.amazonaws.services.s3.model.DeleteBucketRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.msse.seng5811.localstack.LocalstackUtility

object LocalstackS3Utility {
    private val s3 = getS3Client()

    fun getS3Client(): AmazonS3 = buildS3Client(
        endpoint = LocalstackUtility.getEndpoint(),
        region = LocalstackUtility.region,
        awsCredentials = LocalstackUtility.awsCredentials
    )

    fun refreshS3Bucket(name: String) {
        val objectKeys = s3
            .listObjects(name).objectSummaries
            .map { it.key }
            .map { DeleteObjectsRequest.KeyVersion(it) }

        // Delete stale objects from s3 bucket.
        if (objectKeys.isNotEmpty()) {
            s3.deleteObjects(DeleteObjectsRequest(name).withKeys(objectKeys))
        }
    }

    private fun buildS3Client(endpoint: String, region: String, awsCredentials: BasicAWSCredentials): AmazonS3 =
        AmazonS3ClientBuilder.standard()
            .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(awsCredentials.awsAccessKeyId, awsCredentials.awsSecretKey)))
            .withEndpointConfiguration(LocalstackUtility.localStackEndpointConfiguration(endpoint, region))
            .withPathStyleAccessEnabled(true)
            .build()

    private fun createBucket(name: String) {
        val s3Client = getS3Client()
        val createRequest = CreateBucketRequest(name)
        s3Client.createBucket(createRequest)
    }

    private fun deleteBucket(name: String) {
        val s3Client = getS3Client()
        val deleteRequest = DeleteBucketRequest(name)
        s3Client.deleteBucket(deleteRequest)
    }
}