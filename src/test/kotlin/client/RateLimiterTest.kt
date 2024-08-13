package client

import doorman.Doorman
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test

class RateLimiterTest {
    val control_capacity = Channel<Double>()

    val fakeResource: IResource =
        object : IResource {
            override val id: String = "fake"
            override val client: DoormanClient?
                get() = TODO("Not yet implemented")
            override val wants: Double = 500.0
            override val capacity: Channel<Double> = control_capacity
            override val priority: Long = 0
            override var lease: Doorman.Lease? = null
            override suspend fun ask(capacity: Double): Throwable? = null

            override suspend fun release(): Throwable? = null
            override suspend fun expiry(): Long {
                TODO("Not yet implemented")
            }
        }

    @Test
    fun `test rate limiter`() =
        runBlocking {
            val rateLimiter = RateLimiter(fakeResource)
            control_capacity.send(20.0) // 20 RPS
            val start = System.currentTimeMillis()
            println("Start: $start")
            // define a coroutine context
//            CoroutineScope(Dispatchers.Default).launch {
//                delay(5000)
//                rateLimiter.close()
//            }
            println("Now Rate limiting... ")
            val result = rateLimiter.wait()
            val end = System.currentTimeMillis()
            println("Wait: $result , Time taken: ${end - start}") // Should be around 50ms as we are waiting to manage a rate of 10 RPS
            assert(end - start < 250)
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

            val successCounter = mutableListOf<Pair<Int,Long>>()
            for (i in 1..100) {
                val loop_start = System.currentTimeMillis()
                val result = rateLimiter.wait()
                if (result) successCounter.add(Pair(i,System.currentTimeMillis() - loop_start )) else null
                println("Wait: $result at $i , Time taken: ${System.currentTimeMillis() - loop_start}")
            }
            val end = System.currentTimeMillis()
            println("Time taken: ${end - start}")
            println("Success Counter Size: ${successCounter.size}")
            println("Avg Time taken: ${successCounter.map { it.second }.average()}")
            assert(successCounter.size > 85)
        }

    @Test
    fun `test rate limiter with change of capacity`() =
        runBlocking {
            val rateLimiter = RateLimiter(fakeResource)
            control_capacity.send(10.0) // 10 RPS
            val start = System.currentTimeMillis()
            // Map -> { RPS1 : [ ( attempt_number , durationInMilli ),.. ] , RPS2: [ ( attempt_number , durationInMilli ),.. ] }
            val successMap = mutableMapOf<String, MutableList<Pair<Int,Long>>>()
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
                rateLimiter.close()
            }
            println("Now Rate limiting... ")
            var ctr: Int = 0
            while (rateLimiter.isActive) {
                val loop_start = System.currentTimeMillis()
                val result = rateLimiter.wait()
                if(result) {
                    val k = rateLimiter.currentInterval().toString()
                    successMap.getOrPut(k,  { mutableListOf() })
                        .add(Pair(++ctr, System.currentTimeMillis() - loop_start))
                }
                println("Success Map => ${successMap.map { it.key to it.value.size }}")
                println("Wait: $result , Time taken: ${System.currentTimeMillis() - loop_start}")
            }
            println("Average Time taken: ${successMap.map { it.key to it.value.map { it.second }.average() }}")
            assert( successMap["PT0.1S"]?.size!! >= 25) // Ideally should be 30, 10 RPS for 3 seconds
            assert( successMap["PT0.05S"]?.size!! >= 50) // Ideally should be 60, 20 RPS for 3 seconds
        }
}
