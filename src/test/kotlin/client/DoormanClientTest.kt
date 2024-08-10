package client

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test

class DoormanClientTest {

    @Test
    fun `test with actual doorman resource`(): Unit = runBlocking(CoroutineName("ash-test-3")) {
        val client = DoormanClient.create("apple-client")
        val fResourceApples = client.requestResource("proportional",10.0)
        println("Resource created: $fResourceApples")
        val appleRateLimiter = RateLimiter(fResourceApples)
        println("Rate limiter created: $appleRateLimiter")
        // define a coroutine context to kill the rate limiter after 3 seconds
        CoroutineScope(Dispatchers.Default).launch {
            delay(2000)
            println("Closing the rate limiter and client")
            appleRateLimiter.close()
            client.close()
        }

        println("Now Rate limiting... ")
        val start = System.currentTimeMillis()
        println("Start: $start")
        val result = appleRateLimiter.wait()
        println("Wait: $result , Time taken: ${System.currentTimeMillis() - start}") // Should be around 100ms as we are waiting to manage a rate of 10 RPS

    }

}