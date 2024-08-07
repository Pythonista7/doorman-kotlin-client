package client

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import kotlin.math.min

class RateLimiter(private val resource: Resource) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val mutex = Mutex()

    private val events = Channel<Channel<Channel<Boolean>>>()

    // interval indicates period of time once per which the
    // rate limiter releases co-routines waiting for the access to the resource.
    private var interval: Duration = Duration.ofSeconds(1)

    // rate is a limit at which rate limiter releases waiting co-routines
    // that want to access the resource.
    private var rate: Int = 0

    // subintervals is the number of subintervals.
    private var subintervals: Int = 1

    init {
        scope.launch { run() }
    }

    private val isUnlimited get() = rate < 0
    private val isBlocked get() = rate == 0

    val isActive get() = scope.isActive

    suspend fun currentInterval(): Duration {
        mutex.lock()
        var iv : Duration = interval
        mutex.unlock()
        return iv
    }

    suspend fun wait(): Boolean {
        return try {
            withContext(scope.coroutineContext) {
                val response = Channel<Channel<Boolean>>()
                events.send(response) // Tricky! This is a channel that is sent in the events channel
                val unfreeze =
                    response.receive() // Tricky! This is the value received from the channel sent by the function generating events

                val job = this.coroutineContext.job

                val success = select {

                    unfreeze.onReceiveCatching { uf ->
                        uf.getOrNull() ?: throw uf.exceptionOrNull() ?: throw CancellationException("Unfreeze channel is closed")
                        println("Received a value from the unfreeze channel")
                        if (unfreeze.isClosedForReceive) {
                            println("Channel is closed , receive:${unfreeze.isClosedForReceive} , send:${unfreeze.isClosedForSend} or failed. This indicated that the rate limiter is closed.")
                            false
                        } else {
                            println("Channel is open and waiting successful...")
                            true
                        }
                    }

                    job.onJoin {
                        println("Job is joined")
                        false
                    }
                }
                return@withContext success

            }
        } catch (ce: CancellationException) {
            println("Cancellation exception caught: $ce")
            return false
        } catch (e: Exception) {
            println("Generic Exception caught: $e")
            return false
        }
    }

    suspend fun close() {
        this.scope.coroutineContext.job.cancelAndJoin()
    }

    private fun recalculateRate(rate: Int, intervalMilliSeconds: Int): Triple<Int, Int, Duration> {
        println("[RecalculateRate Input]  Rate: $rate, IntervalMilliSeconds: $intervalMilliSeconds")
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
        return when {
            capacity < 0 -> {
                rate = -1
                0
            }
            capacity == 0.0 -> {
                rate = 0
                0
            }
            capacity <= 10 -> {
                println("Capacity is less than 10")
                val (newRate, leftoverRate, newInterval) = recalculateRate(1, (1000 / capacity).toInt())
                rate = newRate
                interval = newInterval
                leftoverRate
            }
            else -> {
                println("Capacity is greater than 10")
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

        var unfreeze = Channel<Boolean>()

        while (true) {
            // we don't really read and use data from this channel, this is more like a valve.
            val wakeUp = Channel<Boolean>()
            println("[Run Loop ts: ${System.currentTimeMillis()}] isBlocked: $isBlocked, isUnlimited: $isUnlimited, rate: $rate, interval: $interval, subintervals: $subintervals, released: $released, leftoverRate: $leftoverRate")
            if (isBlocked.not() && isUnlimited.not()) {
                CoroutineScope(Dispatchers.IO).launch {
                    println("[Sleep-Start] Sleep starts for $interval at ${System.currentTimeMillis()}")
                    delay(interval.toMillis())
                    println("[Sleep-End] Waking up after $interval at ${System.currentTimeMillis()}")
                    wakeUp.send(true)
                    println("[WakeUp] Sent wakeUp signal")
                }
            }

            select {

                resource.capacity.onReceive {
                    capacity ->
                    println("Received capacity: $capacity for resource: ${resource.id}")
                    // Updates rate and interval according to received capacity value from doorman
                    leftoverRateOriginal = updateRate(capacity)

                    // Set released to 0, as a new cycle of co routines' releasing begins.
                    released = 0
                    leftoverRate = leftoverRateOriginal
                    println("Received capacity: $capacity for resource: ${resource.id} , Rate: $rate, Interval: $interval, Subintervals: $subintervals, Released: $released, LeftoverRate: $leftoverRate")
                    return@onReceive 0
                }

                events.onReceive {
                    response ->
                        // If the rate limiter is unlimited, we send back a channel on which
                        // we will immediately send something, unblocking the call to Wait
                        // that it sent there.
                        if(isUnlimited) {
                            val nonBlocking = Channel<Boolean>()
                            response.send(nonBlocking)
                            nonBlocking.send(true)
                            return@onReceive 0
                        }
                        response.send(unfreeze)
                }

                wakeUp.onReceive {
                    // Release waiting goroutines when timer is expired.
                    var max = rate
                    println("[WakeUp: $it] -> Rate: $rate, Subintervals: $subintervals, Released: $released, LeftoverRate: $leftoverRate")

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

                    for(i in 0 until max) {
                        select {
                            unfreeze.onSend(true) {
                                // We managed to release a goroutine
                                // waiting on the other side of the channel.
                                println("Released a coroutine")
                            }
                            // default:
                            // We failed to send value through channel, so nobody
                            // is waiting for the resource now, but we keep trying
                            // sending values via channel, because a waiting goroutine
                            // could eventually appear before rate is reached
                            // and we have to release it.
                        }
                    }

                    return@onReceive 0
                }

            }

        }
    }
}

