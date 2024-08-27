import client.IResource
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import kotlin.math.min

interface RateLimiter {
    suspend fun wait()

    fun forceStop()

    fun isActive(): Boolean

    fun currentInterval(): Long

    suspend fun close()
    }

/*
TODO:
> Have a default rate of along with the resource as a constructor param and when/if doorman goes down we can still have a default rate.
 */

class QpsRateLimiter(private val resource: IResource) : RateLimiter {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.Default + job)
    private val quit = Channel<Unit>()
    private val requestChannel = Channel<CompletableDeferred<Unit>>()
    private var interval: Long = 0

    private var rate: Int = 0
    private var subintervals: Int = 1

    val resourceId: String = resource.id

    private var stop = false

    init {
        scope.launch { run() }
    }

    override suspend fun wait() {
        try {
            if (stop) throw CancellationException("RateLimiter $resourceId is stopped!")
            val request = CompletableDeferred<Unit>()
            requestChannel.send(request)
            request.await()
        } catch (e: CancellationException) {
            println("ERROR: The wait context was cancelled! \n$e")
        }
    }

    override fun forceStop() {
        println("Force stopping!!! RateLimiter $resourceId")
        stop = true
    }

    override fun isActive(): Boolean {
        return !stop
    }

    override fun currentInterval(): Long {
        return interval
    }


    override suspend fun close() {
        println("Closing RateLimiter $resourceId")
        quit.send(Unit)
    }

    private fun recalculate(rate: Int, interval: Int): Triple<Int, Int, Long> {
        var newRate = rate
        var newInterval = interval.toLong()
        var leftoverRate = 0

        if (rate > 1 && interval >= 20) {
            subintervals = min(rate.toDouble(), (interval / 20).toDouble()).toInt()
            newRate = rate / subintervals
            leftoverRate = rate % subintervals
            newInterval = (newRate * interval / rate.toDouble()).toLong()
        }

        return Triple(newRate, leftoverRate, newInterval)
    }

    private fun update(capacity: Double): Int {
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
                val (newRate, leftover, newInterval) = recalculate(1, (1000.0 / capacity).toInt())
                rate = newRate
                interval = newInterval
                leftover
            }
            else -> {
                val (newRate, leftover, newInterval) = recalculate(capacity.toInt(), 1000)
                rate = newRate
                interval = newInterval
                leftover
            }
        }
    }

    private suspend fun run() {
        var released = 0
        var leftoverRate = 0
        var leftoverRateOriginal = 0
        var lastReleaseTime = System.nanoTime()

        while (!stop) {
            select<Unit> {
                quit.onReceive {
                    stop = true
                    requestChannel.consumeEach {
                            request ->
                                request.completeExceptionally(CancellationException("RateLimiter $resourceId is closed!"))

                    }
                    requestChannel.close()
                    quit.close()
                    this@QpsRateLimiter.scope.coroutineContext.cancelChildren()
                    this@QpsRateLimiter.scope.coroutineContext.cancel()
                    this@QpsRateLimiter.job.cancel()
                }
                resource.capacity.onReceive { capacity ->
                    leftoverRateOriginal = update(capacity)
                    released = 0
                    leftoverRate = leftoverRateOriginal
                    lastReleaseTime = System.nanoTime()
                }
                requestChannel.onReceive { request ->
                    val now = System.nanoTime()
                    val elapsedNanos = now - lastReleaseTime
                    val expectedInterval = interval * 1_000_000 // Convert ms to ns

                    if (elapsedNanos >= expectedInterval) {
                        val maxToRelease = rate + if (released < subintervals && leftoverRate > 0) {
                            val stepLeftoverRate = leftoverRate / rate + 1
                            leftoverRate -= stepLeftoverRate
                            stepLeftoverRate
                        } else 0

                        repeat(maxToRelease) {
                            request.complete(Unit)
                            if (requestChannel.isEmpty) return@repeat
                        }

                        lastReleaseTime = now
                        if (++released >= subintervals) {
                            released = 0
                            leftoverRate = leftoverRateOriginal
                        }
                    } else {
                        scope.launch {
                            delay((expectedInterval - elapsedNanos) / 1_000_000) // Convert ns to ms
                            requestChannel.send(request)
                        }
                    }
                }
            }
        }
        return
    }
}


fun newQPS(resource: IResource): QpsRateLimiter = QpsRateLimiter(resource)