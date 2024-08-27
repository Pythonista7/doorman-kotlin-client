package client

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.selects.selectUnbiased
import kotlinx.coroutines.sync.Mutex
import java.time.Duration
import kotlin.math.min

class RateLimiter(private val resource: IResource) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.IO + job)

    private val mutex = Mutex()

    private val events = Channel<Channel<Channel<Boolean>>>(1)

    // interval indicates period of time once per which the
    // rate limiter releases co-routines waiting for the access to the resource.
    private var interval: Duration = Duration.ofSeconds(1)

    // rate is a limit at which rate limiter releases waiting co-routines
    // that want to access the resource.
    private var rate: Int = 0

    // subintervals is the number of subintervals.
    private var subintervals: Int = 1

    init {
        CoroutineScope(Dispatchers.Unconfined + job).launch(CoroutineName("RatelimiterRunLoop")) { run() }
    }

    private val isUnlimited get() = rate < 0
    private val isBlocked get() = rate == 0

    val isActive get() = scope.isActive

    val resourceId = resource.id + "_" + resource.hashCode()

    suspend fun currentInterval(): Duration {
        mutex.lock()
        var iv : Duration = interval
        mutex.unlock()
        return iv
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun wait(): Boolean = withContext(scope.coroutineContext) {
        try {
                val response = Channel<Channel<Boolean>>()
                println("[RL ${this@RateLimiter.resourceId}] Waiting for the rate limiter to release the resource")
//                scope.launch(start = CoroutineStart.UNDISPATCHED){
                println("[RL ${this@RateLimiter.resourceId}] Can send on events? isClosedForSend:${events.isClosedForSend} , isClosedForReceive:${events.isClosedForReceive}, isEmpty:${events.isEmpty}")
                val sendTry = events.trySendBlocking(response) // Tricky! This is a channel that is sent in the events channel
                println("[RL ${this@RateLimiter.resourceId}] Sent a channel to the events channel, waiting to receive unfreeze response. Event Send Result Success: ${sendTry.isSuccess}")
//                }
                val unfreeze = response.receive() // Tricky! This is the value received from the channel sent by the function generating events
//                println("[RL ${this@RateLimiter.resourceId}] Unfreeze Result: ${unfreezeResult.getOrNull()}")
//                val unfreeze = unfreezeResult.getOrThrow()
                println("[RL ${this@RateLimiter.resourceId}] Received a channel from the events channel for listening to unfreeze")

                val job = this@RateLimiter.scope.coroutineContext.job

                val success = select {

                    unfreeze.onReceiveCatching { uf ->
                        uf.getOrNull() ?: throw uf.exceptionOrNull() ?: throw CancellationException("Unfreeze channel is closed")
                        println("[RL ${this@RateLimiter.resourceId}] Received a value from the unfreeze channel")
                        if (unfreeze.isClosedForReceive) {
                            println("[RL ${this@RateLimiter.resourceId}] Channel is closed , receive:${unfreeze.isClosedForReceive} , send:${unfreeze.isClosedForSend} or failed. This indicated that the rate limiter is closed.")
                            false
                        } else {
                            println("[RL ${this@RateLimiter.resourceId}] Channel is open and waiting successful...")
                            true
                        }
                    }

                    job.onJoin {
                        println("[RL ${this@RateLimiter.resourceId}] Job is joined")
                        false
                    }
                }
                return@withContext success

        } catch (ce: CancellationException) {
            println("[RL ${this@RateLimiter.resourceId}] Cancellation exception caught: $ce")
            return@withContext false
        } catch (e: Exception) {
            println("[RL ${this@RateLimiter.resourceId}] Generic Exception caught: $e")
            return@withContext  false
        }
    }

    suspend fun close() {
        this.scope.coroutineContext.job.cancelAndJoin()
    }

    private fun recalculateRate(rate: Int, intervalMilliSeconds: Int): Triple<Int, Int, Duration> {
        println("[RL ${this@RateLimiter.resourceId}] [RecalculateRate Input]  Rate: $rate, IntervalMilliSeconds: $intervalMilliSeconds")
        var newRate = rate
        var newInterval = Duration.ofMillis(intervalMilliSeconds.toLong())
        var leftoverRate = 0

        // If the rate limit is more than 2 Hz we are going to divide the given
        // interval to some number of subintervals and distribute the given rate
        // among these subintervals to avoid burstiness which could take place otherwise.
        if (rate > 1 && intervalMilliSeconds >= 20) {
            val m =  min(rate.toFloat(), (intervalMilliSeconds.toFloat()/ 20.0).toFloat())
            subintervals = m.toInt()
            newRate = rate / subintervals
            leftoverRate = rate % subintervals
            val adjustedIntervalMilliSeconds = (newRate * intervalMilliSeconds).toFloat() / rate
            newInterval = Duration.ofMillis(adjustedIntervalMilliSeconds.toLong())
        }

        return Triple(newRate, leftoverRate, newInterval)
    }

    private fun updateRate(capacity: Double): Int {
        println("[RL ${this@RateLimiter.resourceId}] [UpdateRate] New Capacity: $capacity")
        return when {
            capacity < 0 -> {
                rate = -1
                0
            }
            capacity == 0.0 -> {
                println("[RL ${this@RateLimiter.resourceId}] Capacity $capacity is 0 so rate is set to 0")
                rate = 0
                0
            }
            capacity <= 10 -> {
                println("[RL ${this@RateLimiter.resourceId}] Capacity $capacity is less than 10")
                val (newRate, leftoverRate, newInterval) = recalculateRate(1, (1000 / capacity).toInt())
                rate = newRate
                interval = newInterval
                leftoverRate
            }
            else -> {
                println("[RL ${this@RateLimiter.resourceId}] Capacity $capacity is greater than 10")
                val (newRate, leftoverRate, newInterval) = recalculateRate(capacity.toInt(), 1000)
                rate = newRate
                interval = newInterval
                leftoverRate
            }
        }
    }



    private suspend fun run() {
        var released = 0
        var leftoverRate = 0
        var leftoverRateOriginal = 0

        while (true) {
            val unfreeze = Channel<Boolean>()
            println("[RL: ${this@RateLimiter.resourceId}] [RL Run Loop ts: ${System.currentTimeMillis()}] isBlocked: $isBlocked, isUnlimited: $isUnlimited, rate: $rate, interval: $interval, subintervals: $subintervals, released: $released, leftoverRate: $leftoverRate")
            // we don't really read and use data from this channel, this is more like a valve.
            val wakeUp = Channel<Boolean>()

            withContext(scope.coroutineContext) {
                if (isBlocked.not() && isUnlimited.not()) {
                    println("[RL: ${this@RateLimiter.resourceId}] Start] Sleep starts for $interval at ${System.currentTimeMillis()}")
                    delay(interval.toMillis())
                    println("[RL: ${this@RateLimiter.resourceId}] [Sleep-End] Waking up after $interval at ${System.currentTimeMillis()}")
                    wakeUp.send(true)
                    println("[RL: ${this@RateLimiter.resourceId}] [Sleep-End-Send] True")
                } else {
                    null
                }
            }

            // val tick = ticker(delayMillis = 1000, initialDelayMillis = 1000, context = scope.coroutineContext, mode = TickerMode.FIXED_DELAY)

            selectUnbiased {

                resource.capacity.onReceive {
                    capacity ->
//                    val capacity = c.getOrNull()
//                    if(capacity == null) {
//                        println("[ERROR] [RL ${this@RateLimiter.resourceId}] : Received capacity is null")
//                        return@onReceiveCatching 0
//                    }
                    println("[RL ${this@RateLimiter.resourceId}] Received capacity: $capacity for resource: ${this@RateLimiter.resourceId}")
                    // Updates rate and interval according to received capacity value from doorman
                    leftoverRateOriginal = updateRate(capacity)
                    println("[RL ${this@RateLimiter.resourceId}] Updated rate: $rate, interval: $interval, subintervals: $subintervals, Released: $released, LeftoverRate: $leftoverRate")
                    // Set released to 0, as a new cycle of co routines' releasing begins.
                    released = 0
                    leftoverRate = leftoverRateOriginal
                    println("[RL ${this@RateLimiter.resourceId}] Received capacity: $capacity for resource: ${this@RateLimiter.resourceId} , Rate: $rate, Interval: $interval, Subintervals: $subintervals, Released: $released, LeftoverRate: $leftoverRate")

                }

                events.onReceive { response ->
                    // If the rate limiter is unlimited, we send back a channel on which
                    // we will immediately send something, unblocking the call to Wait
                    // that it sent there.
                        try {
                            println("[RL ${this@RateLimiter.resourceId} [Events] Received a channel from the events channel")
                            scope.launch {
                                println("[RL ${this@RateLimiter.resourceId}] [Events] sending unfreeze...")
                                if (isUnlimited) {
                                    val nonBlocking = Channel<Boolean>()
                                    response.send(nonBlocking)
                                    nonBlocking.send(true)
                                }

                                response.send(unfreeze)
                                println("[RL ${this@RateLimiter.resourceId}] [Events] Sent a channel to the events channel for listening to unfreeze")
                            }
                        } catch (e: Exception) {
                            println("[RL ${this@RateLimiter.resourceId}] Error sending unfreeze: ${e.message}")
                        } finally {
                            println("[RL ${this@RateLimiter.resourceId}] [Events] Finally block executed")
                        }
                }

                wakeUp.onReceive {
                    // Release waiting goroutines when timer is expired.
                    println("[RL WakeUp: ${this@RateLimiter.resourceId}] -> has been woken up")
                    var max = rate
                    println("[RL WakeUp:  ${this@RateLimiter.resourceId}] -> Rate: $rate, Subintervals: $subintervals, Released: $released, LeftoverRate: $leftoverRate")

                    if(released < subintervals) {
                        if(leftoverRate > 0) {
                            val stepLeftoverRate = leftoverRate / rate + 1
                            max += stepLeftoverRate
                            leftoverRate -= stepLeftoverRate
                        }
                        released++
                    } else {
                        released = 0
                        leftoverRate = leftoverRateOriginal
                    }
                    println("[RL WakeUp: $resourceId] Unfreezing $max coroutines to make requests")
                    for(i in 0 until max) {
                        // select {
                            unfreeze.send(true)
                            // We managed to release a goroutine
                            // waiting on the other side of the channel.
                            println("[RL: ${this@RateLimiter.resourceId}] UnfreezeSend, Released a coroutine : $it")

                            // default:
                            // We failed to send value through channel, so nobody
                            // is waiting for the resource now, but we keep trying
                            // sending values via channel, because a waiting goroutine
                            // could eventually appear before rate is reached
                            // and we have to release it.
                        // }
                    }

                    return@onReceive 0
                }

            }
            // intervalUpdateJob?.join()
        }
        println("[RL ${this@RateLimiter.resourceId}] Rate limiter is closed?? run while() stopped!")
    }
}

