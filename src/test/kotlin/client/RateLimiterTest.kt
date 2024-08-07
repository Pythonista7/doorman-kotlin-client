package client

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.junit.jupiter.api.Test

class RateLimiterTest {
    val control_capacity = Channel<Double>()

    val fakeResource: Resource =
        object : Resource {
            override val id: String = "fake"
            override val wants: Double = 500.0
            override val capacity: ReceiveChannel<Double> = control_capacity

            override suspend fun ask(capacity: Double): Result<Unit> = Result.success(Unit)

            override suspend fun release(): Result<Unit> = Result.success(Unit)
        }

    @Test
    fun `test rate limiter`() =
        runBlocking {
            val rateLimiter = RateLimiter(fakeResource)
            control_capacity.send(10.0) // 10 RPS
            val start = System.currentTimeMillis()
            println("Start: $start")
            // define a coroutine context
            CoroutineScope(Dispatchers.Default).launch {
                delay(5000)
                rateLimiter.close()
            }
            println("Now Rate limiting... ")
            val result = rateLimiter.wait()
            val end = System.currentTimeMillis()
            println("Wait: $result , Time taken: ${end - start}") // Should be around 100ms as we are waiting to manage a rate of 10 RPS
        }

    @Test
    fun `test rate of the ratelimiter`() =
        runBlocking {
            val rateLimiter = RateLimiter(fakeResource)
            control_capacity.send(10.0) // 10 RPS
            val start = System.currentTimeMillis()
            println("Start: $start")
            CoroutineScope(Dispatchers.Default).launch {
                delay(10000)
                rateLimiter.close()
            }
            println("Now Rate limiting... ")
            for (i in 1..100) {
                val loop_start = System.currentTimeMillis()
                val result = rateLimiter.wait()
                println("Wait: $result at $i , Time taken: ${System.currentTimeMillis() - loop_start}")
            }
            val end = System.currentTimeMillis()
            println("Time taken: ${end - start}")
        }

    @Test
    fun `test rate limiter with change of capacity`() =
        runBlocking {
            val rateLimiter = RateLimiter(fakeResource)
            control_capacity.send(10.0) // 10 RPS
            val start = System.currentTimeMillis()
            println("Start: $start")

            // Change RPS after 3 seconds
            CoroutineScope(Dispatchers.IO).launch {
                delay(3000)
                println("Changing RPS to 20")
                control_capacity.send(20.0) // 20 RPS
            }
            // Stop job after 6 seconds
            CoroutineScope(Dispatchers.IO).launch {
                delay(6000)
                // coroutineContext.job.cancelAndJoin()
                rateLimiter.close()
            }
            println("Now Rate limiting... ")
            while (rateLimiter.isActive) {
                val loop_start = System.currentTimeMillis()
                val result = rateLimiter.wait()
                println("Wait: $result , Time taken: ${System.currentTimeMillis() - loop_start}")
            }
        }

    @Test
    fun `test with actual doorman resource`() {
        val resource = null // TODO: Create a doorman resource
    }
}
