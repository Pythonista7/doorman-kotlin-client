package client

import client.Utils.Companion.backoff
import doorman.CapacityGrpc
import doorman.Doorman
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import kotlin.coroutines.CoroutineContext
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

const val MIN_REFRESH_INTERVAL = 20L

class DoormanClient private constructor(
    id: String,
) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.IO + job)
    companion object {
        fun create(id: String): DoormanClient {
            val c = DoormanClient(id)
            return c
        }

    }

    val id: String = id

//    private var ready = false
//    private var readyChannel = Channel<Boolean>(1)

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

    private val newResource: Channel<IResourceAction> = Channel<IResourceAction>(2)
    val releaseResource: Channel<IResourceAction> = Channel<IResourceAction>(2)

    init {
        scope.launch { run() }
    }
    fun getMaster(): String = "$master:$port"

    suspend fun requestResource(id: String, capacity: Double): Resource {
        println("[Client ${this.id}][Request Resource] Requesting resource $id with capacity $capacity")
        return this.requestResourceWithPriority(id,capacity,0)
    }

    suspend fun requestResourceWithPriority(id:String, capacity: Double, priority:Long): Resource {
        val resource = Resource(id = id, client = this, wants = capacity, priority = priority)
        val errC: Channel<Throwable?> = Channel()

        val newResourceAction : IResourceAction = object : IResourceAction {
            override val errC: Channel<Throwable?> = errC
            override val resource: IResource = resource
        }
        println("[Client ${this.id}][Request Resource] Sending request at ${System.currentTimeMillis()} for resource $id with capacity $capacity")
        val sendRes = newResource.trySend(newResourceAction)

        println("[Client ${this.id}][Request Resource] Waiting for errC, Sent request for resource $id with capacity $capacity with response: $sendRes")
        return select {
            errC.onReceiveCatching() {
                println("[Client ${this@DoormanClient.id}][Request Resource] Received errC, Sent request for resource $id with capacity $capacity")
                val err = it.getOrNull()
                if (err != null) throw Exception("Error creating resource $id with capacity $capacity: $err")
                else {return@onReceiveCatching resource}
//                it.onSuccess {
//                    println("[Client ${this@DoormanClient.id}][Request Resource] Resource $id with capacity $capacity created successfully")
//                    // return@onReceiveCatching resource
//                }.onFailure { it ->
//                    println("[Client ${this@DoormanClient.id}][Request Resource] Error creating resource $id with capacity $capacity: $it")
//                    throw Exception("Error creating resource $id with capacity $capacity: $it")
//                }.onClosed {
//                    throw Exception("Channel closed while creating resource $id with capacity $capacity: $it")
//                }
            }
        }
    }

    fun close() {
        this.newResource.close(Throwable("Client ${this.id} is closing"))
        this.releaseResource.close(Throwable("Client ${this.id} is closing"))
        this.stop = true
        // Release all held resources
        val releaseCapacityReq = Doorman.ReleaseCapacityRequest.newBuilder().setClientId(this.id)
        for(r in this.resources.values) {
            releaseCapacityReq.addResourceId(r.id)
            r.capacity.close(Throwable("Client ${this.id} is closing"))
        }
        val releaseRes = rpcClient.releaseCapacity(releaseCapacityReq.build())
        println("[Client ${this.id} Close] Release Capacity Response: $releaseRes")
        this.channel.shutdown()
    }

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
    private suspend fun run() {
        val wakeUp = Channel<Boolean>(1) // clientRefreshChannel
        var timerJob: TimerJob? = null
        var retryCount = 0
        try {
            while (!stop) {

                select {

                    newResource.onReceiveCatching { rA ->
                        {
                            println("[Client ${this@DoormanClient.id}] [New Resource] Req to create resource $rA")
                            val resourceAction = rA.getOrNull()
                            if (resourceAction == null) {
                                println("[Client ${this@DoormanClient.id}] Error creating resource: ResourceAction is null!!!")
                            } else {
                                val err = addResource(resourceAction.resource)
                                if (err != null) {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("[Client ${this@DoormanClient.id}] Error adding resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                } else {
                                    CoroutineScope(Dispatchers.IO).launch{ resourceAction.errC.send(null) }
                                }
                            }
                        }
                    }

                    releaseResource.onReceiveCatching { rA ->
                        {
                            println("[Client ${this@DoormanClient.id}] [Release Resource] Req to release resource $rA")
                            val resourceAction: IResourceAction? = rA.getOrNull()
                            if (resourceAction == null) {
                                println("[Client ${this@DoormanClient.id}] Error releasing resource: ResourceAction is null!!!")
                            } else {
                                val err = removeResource(resourceAction.resource)
                                if (err != null) {
                                    CoroutineScope(Dispatchers.IO).launch {
                                        println("[Client ${this@DoormanClient.id}] Error removing resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                } else {
                                    CoroutineScope(Dispatchers.IO).launch{ resourceAction.errC.send(null) }
                                }
                            }
                        }
                    }

                    wakeUp.onReceiveCatching {
                        it.getOrNull() ?: println("[Client ${this@DoormanClient.id}] Got null in waking up channel instead of boolean!")
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

                var interval : Long
                // println("[Client $id] Run Loop at ${System.currentTimeMillis()} performing requests...")
                val (newInterval, newRetryCount) = if(this.resources.isEmpty().not()) {
                    performRequests(retryCount)
                } else {
                    Pair(MIN_REFRESH_INTERVAL.toLong(),retryCount)
                }
                interval = newInterval
                retryCount = newRetryCount

                println("[Client ${this.id}] Looping run() at ${System.currentTimeMillis()}, stop = $stop , isClosedForReceive:${newResource.isClosedForReceive}")
                timerJob = TimerJob()
                timerJob!!.startTimer(
                    delayMillis = interval,
                    action = { wakeUp.send(true) },
                )
            }
        } catch (e: Exception) {
            println("[Client ${this@DoormanClient.id}] Error in run: $e")
        } finally {
            println("[Client ${this@DoormanClient.id}] Closing client")
            this.close()
        }
    }

    private fun addResource(resource: IResource): Throwable? {
        if (resources.keys.contains(resource.id)) {
            println("[ERROR][Client ${this@DoormanClient.id}] Resource with id ${resource.id} already exists")
            return DuplicateResourceError("[Client ${this@DoormanClient.id}] Resource with id ${resource.id} already exists")
        }
        println("[Client ${this.id}][Add Resource] Adding resource $resource")
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
        println("[$id : Perform Requests] Performing requests at ${System.currentTimeMillis()}")

        // Create a get capacity request in bulk for all resources to reduce QPS to doorman servers
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
            println("[$id : Perform Requests] Retrying with retryCount $retryCount for $getCapacityReq")
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
                        println("[$id : Perform Requests] Resource ${r.id} has expired, releasing by setting capacity to 0.")
                        r.capacity.send(0.0)
                        println("[$id : Perform Requests] Resource ${r.id} has been released on expiry.")
                    }
                }
            }
            println("[$id : Perform Requests] Backing off for retry: $retryCount")
            val bkOff =  backoff(
                baseDelay = 1.seconds, // TODO: Make this configurable
                maxDelay = 10.minutes, // TODO: Make this configurable
                retries = retryCount + 1
            )
            return Pair(bkOff,retryCount + 1)
        }

        println("[$id : Perform Requests] Capacity Response: $capacityResponse")

        // update client state with the response capacity and lease;
        for(r in capacityResponse.responseList) {
            val clientResource = this.resources[r.resourceId]
            if (clientResource == null){
                println("ERROR [$id : Perform Requests] Resource ${r.resourceId} not found in client resources")
                continue
            }
            val oldCapacity:Double = clientResource.lease?.capacity ?: -1.0

            // update lease
            clientResource.lease = r.gets

            // Only send a message down the channel if the capacity has changed.
            if(oldCapacity != r.gets.capacity) {
                CoroutineScope(Dispatchers.IO).launch {
                    println("[$id : Perform Requests] Sending capacity update for ${r.resourceId}, from $oldCapacity to ${r.gets.capacity}")
                    clientResource.capacity.trySend(r.gets.capacity)
                    println("[$id : Perform Requests] Capacity update sent for ${r.resourceId} , $oldCapacity -> ${r.gets.capacity}")
                }
            } else {
                println("[$id : Perform Requests] Capacity for ${r.resourceId} has not changed, not sending update")
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
