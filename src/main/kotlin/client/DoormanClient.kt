package client

import client.Utils.Companion.backoff
import doorman.CapacityGrpc
import doorman.Doorman
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

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

const val MIN_REFRESH_INTERVAL = 100

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

    val resources: MutableMap<String, IResource> = mutableMapOf()

    private var master: String = "localhost"
    private var port: Int = 15000

    private var channel: ManagedChannel =
        ManagedChannelBuilder
            .forAddress(master, port)
            .usePlaintext()
            .build()

    private var stop = false

    private var rpcClient: CapacityGrpc.CapacityBlockingStub = CapacityGrpc.newBlockingStub(channel)

    private val newResource: Channel<IResourceAction> = Channel()
    val releaseResource: Channel<IResourceAction> = Channel()

    fun getMaster(): String = "$master:$port"

    private fun refreshMaster() {
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
    @OptIn(ExperimentalCoroutinesApi::class)
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
                            val resourceAction = rA.getOrNull()
                            if (resourceAction == null)
                                {
                                    println("Error creating resource: ResourceAction is null!!!")
                                } else {
                                val err = addResource(resourceAction.resource)
                                if (err != null) {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("Error adding resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                }
                            }
                        }
                    }

                    releaseResource.onReceiveCatching { rA ->
                        {
                            println("[Release Resource] Req to release resource $rA")
                            val resourceAction: IResourceAction? = rA.getOrNull()
                            if (resourceAction == null) {
                                println("Error releasing resource: ResourceAction is null!!!")
                            } else {
                                val err = removeResource(resourceAction.resource)
                                if (err != null) {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("Error removing resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                }
                            }
                        }
                    }

                    wakeUp.onReceiveCatching {
                        println("[Wake Up] Waking up at ${System.currentTimeMillis()}")
                        it.getOrNull() ?: println("Got null in waking up channel instead of boolean!")
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

    private fun addResource(resource: IResource): Throwable? {
        if (resources.keys.contains(resource.id)) {
            return DuplicateResourceError("Resource with id ${resource.id} already exists")
        }
        println("[Add Resource] Adding resource $resource")
        resources[resource.id] = resource
        return null
    }

    private fun removeResource(resource: IResource): Throwable? {
        println("[Remove Resource] Removing resource $resource")
        if (resources.keys.contains(resource.id).not()) {
            return null
        }
        resources.remove(resource.id)
        resource.capacity.cancel(CancellationException("Closing capacity as resource ${resource.id} is being removed"))

        val releaseCapacityReq =
            Doorman.ReleaseCapacityRequest
                .newBuilder()
                .setClientId(this.id)
                .addResourceId(resource.id)
                .build()

        try {
            val response = rpcClient.releaseCapacity(releaseCapacityReq)
            println("Release Capacity Response: $response")
            if(response.mastership.masterAddress == null) {
                return MasterUnknownError("Master address is null in response")
            }
            if(response.mastership.masterAddress != getMaster()) {
                refreshMaster()
                // retry release
                val retryResponse = rpcClient.releaseCapacity(releaseCapacityReq)
                if (retryResponse.mastership.masterAddress != getMaster() || retryResponse.mastership.masterAddress == null) {
                    return Exception("Master address mismatch after retry, expected ${getMaster()} but rpc says master is $retryResponse")
                }
            }
        } catch (e: Exception) {
            return e
        }

        return null
    }

    /*
     * performRequests performs the actual RPCs to the server. It returns the interval in Milli and new retryCount
     */
    private fun performRequests(retryCount: Int): Pair<Long, Int> {
        println("[Perform Requests] Performing requests at ${System.currentTimeMillis()}")

        // Create a get capacity request
        val getCapacityReq = Doorman.GetCapacityRequest.newBuilder().setClientId(this.id)

        for(r in resources.values) {
            getCapacityReq.addResource(
                Doorman.ResourceRequest
                    .newBuilder()
                    .setResourceId(r.id)
                    .setPriority(r.priority)
                    .setWants(r.wants)
                    .setHas(r.lease)
                    .build()
            )
        }

        if(retryCount > 0) {
            println("[Perform Requests] Retrying with retryCount $retryCount for $getCapacityReq")
        }

        val capacityResponse = try {
            rpcClient.getCapacity(getCapacityReq.build())
        } catch (e: Exception) {
            println("Error getting capacity: $e")
            // Expired resources only need to be handled if the
            // RPC failed: otherwise the client has gotten a
            // refreshed lease.
            for (r in resources.values) {
                if (r.lease != null && r.lease!!.expiryTime < System.currentTimeMillis()) {
                    CoroutineScope(Dispatchers.IO).launch{
                        println("[Perform Requests] Resource ${r.id} has expired, releasing by setting capacity to 0.")
                        r.capacity.send(0.0)
                        println("[Perform Requests] Resource ${r.id} has been released on expiry.")
                    }
                }
            }
            val bkOff =  backoff(
                baseDelay = 1.seconds, // TODO: Make this configurable
                maxDelay = 10.minutes, // TODO: Make this configurable
                retries = retryCount + 1
            )
            return Pair(bkOff,retryCount + 1)
        }

        // update client state with the response capacity and lease;
        for(r in capacityResponse.responseList) {
            val clientResource = this.resources[r.resourceId]
            if (clientResource == null){
                println("ERROR [Perform Requests] Resource ${r.resourceId} not found in client resources")
                continue
            }
            val oldCapacity:Double = clientResource.lease?.capacity ?: -1.0

            // update lease
            clientResource.lease = r.gets

            // Only send a message down the channel if the capacity has changed.
            if(oldCapacity != r.gets.capacity) {
                CoroutineScope(Dispatchers.IO).launch {
                    println("[Perform Requests] Sending capacity update for ${r.resourceId}, from $oldCapacity to ${r.gets.capacity}")
                    clientResource.capacity.send(r.gets.capacity)
                    println("[Perform Requests] Capacity update sent for ${r.resourceId} , $oldCapacity -> ${r.gets.capacity}")
                }
            }
        }

        // Figure out refresh interval, find the minimum refresh interval
        var interval = 15.minutes // some long duration which should get overwritten
        for( r in capacityResponse.responseList) {
            val refresh = r?.gets?.refreshInterval?.milliseconds ?: 0.milliseconds
            if(refresh < interval) {
                interval = refresh
            }
        }

        if(interval.inWholeMilliseconds < MIN_REFRESH_INTERVAL) {
            interval = MIN_REFRESH_INTERVAL.milliseconds
        }

        return Pair(interval.inWholeMilliseconds, 0)
    }
}
