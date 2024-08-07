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

    fun startTimer(delayMillis: Long, action: suspend () -> Unit) {
        timerJob?.cancel() // Cancel any existing timer
        timerJob = scope.launch {
            delay(delayMillis)
            action()
        }
    }

    fun stopTimer():Boolean {
        try {
            timerJob?.cancel()
            timerJob = null
            return true
        } catch (e: Exception) {
            println("Error stopping timer: $e")
            return false
        }

    }

    fun isActive(): Boolean {
        return timerJob?.isActive ?: false
    }

    fun close() {
        stopTimer()
        scope.cancel()
    }

}

class DoormanClient {

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


    fun refreshMaster() {
        val discoveredMaster = discoverMaster()
            ?: throw Exception("Could not discover master") // TODO: Log error and retain old rate limits ,throwing for now

        if (discoveredMaster.isMaster.not()) {
            val newMaster  = discoveredMaster.mastership.masterAddress
            try {
                master = newMaster.split(":")[0]
                port = newMaster.split(":")[1].toInt()
            } catch (e: Exception) {
                throw Exception("Could not parse master address $newMaster")
            }
        }

        // register client with new master
        channel = ManagedChannelBuilder
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
                    newResource.onReceiveCatching {
                        resourceAction -> {
                            // TODO;
                        }
                    }

                    releaseResource.onReceiveCatching {
                        resourceAction -> {
                            // TODO;
                        }
                    }

                    wakeUp.onReceiveCatching {
                        timerJob = null
                        return@onReceiveCatching 0
                    }
                }
                if (timerJob != null && timerJob!!.stopTimer() ) {
                    while(wakeUp.isEmpty.not()){ wakeUp.receive() }
                    timerJob = null
                }

                // var interval : Duration
                val (interval, retryCount )= performRequests(retryCount)

                timerJob = TimerJob()
                timerJob!!.startTimer(
                        delayMillis = interval,
                        action = { wakeUp.send(true) }
                    )
            }
        } catch (e: Exception) {
            println("Error in run: $e")
        } finally {
            stop = true
        }
    }

    private fun performRequests(retryCount:Int) : Pair<Long, Int> {
        return Pair(1000, 0)
    }
}