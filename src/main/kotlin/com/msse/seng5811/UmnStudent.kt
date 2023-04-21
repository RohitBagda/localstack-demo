package com.msse.seng5811

import java.time.Instant

data class UmnStudent(
    val umnApplicant: UmnApplicant,
    val umnId: String,
    val admissionTimeStamp: Instant
)