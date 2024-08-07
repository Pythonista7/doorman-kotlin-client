package client

import doorman.CapacityGrpc
import doorman.Doorman
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select

private class TimerJob {
    private var timerJob: Job? = null
    private val scope = CoroutineScope(Dispatchers.Default)

    fun startTimer(
        delayMillis: Long,
        action: suspend () -> Unit,
    ) {
        timerJob?.cancel() // Cancel any existing timer
        timerJob =
            scope.launch {
                delay(delayMillis)
                action()
            }
    }

    fun stopTimer(): Boolean {
        try {
            timerJob?.cancel()
            timerJob = null
            return true
        } catch (e: Exception) {
            println("Error stopping timer: $e")
            return false
        }
    }

    fun isActive(): Boolean = timerJob?.isActive ?: false

    fun close() {
        stopTimer()
        scope.cancel()
    }
}

class DoormanClient(
    id: String,
) {
    companion object {
        fun create(id: String): DoormanClient = DoormanClient(id)
    }

    init {
        CoroutineScope(Dispatchers.IO).launch { run() }
    }

    val id: String = id
    private var master: String = "localhost"
    private var port: Int = 15000

    private var channel: ManagedChannel =
        ManagedChannelBuilder
            .forAddress(master, port)
            .usePlaintext()
            .build()

    private var stop = false

    private var rpcClient: CapacityGrpc.CapacityBlockingStub = CapacityGrpc.newBlockingStub(channel)

    private val newResource: Channel<ResourceAction> = Channel()
    private val releaseResource: Channel<ResourceAction> = Channel()

    fun getMaster(): String = "$master:$port"

    fun refreshMaster() {
        val discoveredMaster =
            discoverMaster()
                ?: throw Exception("Could not discover master") // TODO: Log error and retain old rate limits ,throwing for now

        if (discoveredMaster.isMaster.not()) {
            val newMaster = discoveredMaster.mastership.masterAddress
            try {
                master = newMaster.split(":")[0]
                port = newMaster.split(":")[1].toInt()
            } catch (e: Exception) {
                throw Exception("Could not parse master address $newMaster")
            }
        }

        // register client with new master
        channel =
            ManagedChannelBuilder
                .forAddress(master, port)
                .usePlaintext()
                .build()

        rpcClient = CapacityGrpc.newBlockingStub(channel)
    }

    private fun discoverMaster(): Doorman.DiscoveryResponse? {
        val discoveryRequest = Doorman.DiscoveryRequest.newBuilder().build()
        val discoverResponse = this.rpcClient.discovery(discoveryRequest)
        return discoverResponse
    }

    // run is the client's main loop. It takes care of requesting new
    // resources, and managing ones already claimed. This is the only
    // method that should be modifying the client's internal state and
    // performing RPCs.
    private suspend fun run() {
        val wakeUp = Channel<Boolean>(1) // clientRefreshChannel
        var timerJob: TimerJob? = null
        var retryCount = 0
        try {
            while (!stop) {
                select {
                    newResource.onReceiveCatching { rA ->
                        {
                            println("[New Resource] Req to create resource $rA")
                            val resourceAction = rA.getOrThrow()
                            val err = addResource(resourceAction.resource)
                            if (err != null)
                                {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("Error adding resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                }
                        }
                    }

                    releaseResource.onReceiveCatching { rA ->
                        {
                            println("[Release Resource] Req to release resource $rA")
                            val resourceAction = rA.getOrThrow()
                            val err = removeResource(resourceAction.resource)
                            if (err != null)
                                {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("Error removing resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                }
                        }
                    }

                    wakeUp.onReceiveCatching {
                        println("[Wake Up] Waking up at ${System.currentTimeMillis()}")
                        it.getOrThrow()
                        timerJob = null
                        return@onReceiveCatching 0
                    }
                }
                if (timerJob != null && timerJob!!.stopTimer()) {
                    while (wakeUp.isEmpty.not()) {
                        wakeUp.receive()
                    }
                    timerJob = null
                }

                // var interval : Duration
                val (interval, retryCount) = performRequests(retryCount)

                timerJob = TimerJob()
                timerJob!!.startTimer(
                    delayMillis = interval,
                    action = { wakeUp.send(true) },
                )
            }
        } catch (e: Exception) {
            println("Error in run: $e")
        } finally {
            stop = true
        }
    }

    private fun removeResource(resource: Resource): Throwable? = null

    private fun addResource(resource: Resource): Throwable? = null

    /*
     * performRequests performs the actual RPCs to the server. It returns the interval in Milli and new retryCount
     */
    private fun performRequests(retryCount: Int): Pair<Long, Int>  {
        println("[Perform Requests] Performing requests at ${System.currentTimeMillis()}")
        return Pair(1000, 0)
    }
}
