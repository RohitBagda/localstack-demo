package com.msse.seng5811

import java.time.Instant
import java.util.*

data class UmnStudent(
    val umnApplicant: UmnApplicant,
    val umnId: String = UUID.randomUUID().toString(),
    val admissionTimeStamp: Instant = Instant.now()
)