package client

import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration

class Utils {

    companion object {

        /*
            * Exponential backoff with jitter
            * @param baseDelay: The base delay
            * @param maxDelay: The maximum delay
            * @param retries: The number of retries
            * @return The backoff time in milliseconds
         */
        fun backoff(baseDelay: Duration, maxDelay: Duration, retries: Int): Long {
            val backoff = baseDelay * (1 shl retries) // Exponential backoff
            val jitterBackoff = backoff / 2 + (backoff * Random.nextDouble())
            return min(jitterBackoff.inWholeMilliseconds, maxDelay.inWholeMilliseconds)
        }
    }

}