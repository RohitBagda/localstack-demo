package com.msse.seng5811.localstack

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder

object LocalstackUtility {
    private const val LOCALSTACK_URL_KEY = "LOCALSTACK_URL"
    private const val LOCALSTACK_URL_DEFAULT = "http://127.0.0.1:4566"

    val awsCredentials = BasicAWSCredentials("access-key", "secret-key")
    const val region = "us-east-1"

    fun getEndpoint(): String = System.getenv(LOCALSTACK_URL_KEY) ?: LOCALSTACK_URL_DEFAULT
    fun localStackEndpointConfiguration(endpoint: String, region: String = LocalstackUtility.region) =
        AwsClientBuilder.EndpointConfiguration(endpoint, region)
}