package com.msse.seng5811.utils

import java.time.Duration
import java.time.Instant

/**
 * For testing behavior that is only true after a while.
 *
 * Provide a block of assertions; if the assertions fail (i.e., a Throwable or AssertionError happens), we will continue retrying for
 * a minute, or until the block can run successfully.
 */
fun eventuallyConsistent(assertions: () -> Unit) = within(Duration.ofMinutes(1), assertions)

/**
 * For testing behavior that you expect to be true within a certain amount of time.
 *
 * Just like [eventuallyConsistent] but lets you specify a max duration.
 */
fun within(duration: Duration, assertions: () -> Unit) = assertBy(Instant.now().plus(duration), assertions)

private tailrec fun assertBy(endTime: Instant, assertions: () -> Unit) {
    val result = runCatching(assertions)

    when {
        result.isSuccess -> Unit
        result.isFailure && Instant.now() > endTime ->
            throw AssertionError("Never passed assertions", result.exceptionOrNull())
        else -> {
            Thread.sleep(10)
            assertBy(endTime, assertions)
        }
    }
}