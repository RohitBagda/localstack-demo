plugins {
    kotlin("jvm") version "1.8.0"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    val mockitoVersion = "5.2.0"
    val junitJupiterVersion = "5.9.2"
    val awsSdkVersion = "1.12.445"
    val fasterXmlJacksonVersion = "2.14.2"

    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")

    // Json
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$fasterXmlJacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-json-org:$fasterXmlJacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$fasterXmlJacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$fasterXmlJacksonVersion")
    implementation("javax.xml.bind:jaxb-api:2.3.1")

    // AWS
    implementation("com.amazonaws:aws-java-sdk-kinesis:$awsSdkVersion")
    implementation("com.amazonaws:aws-java-sdk-s3:$awsSdkVersion")
    implementation("com.amazonaws:aws-java-sdk-core:$awsSdkVersion")

    // Test
    testImplementation(kotlin("test"))

    // Mockito
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
    testImplementation("org.mockito:mockito-inline:$mockitoVersion")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")

    // JUnit
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}