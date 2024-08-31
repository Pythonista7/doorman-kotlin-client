package client

import client.Utils.Companion.backoff
import doorman.CapacityGrpc
import doorman.Doorman
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select

import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds


const val MIN_REFRESH_INTERVAL = 20L // 20 milliseconds

class DoormanClient private constructor(
    id: String,
) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.IO + job + CoroutineName("ClientSupervisor"))
    val id: String = id

    val resources: MutableMap<String, IResource> = mutableMapOf()

    private var master: String = "localhost"
    private var port: Int = 15000

    private var channel: ManagedChannel =
        ManagedChannelBuilder
            .forAddress(master, port)
            .usePlaintext()
            .build()

    private var rpcClient: CapacityGrpc.CapacityBlockingStub = CapacityGrpc.newBlockingStub(channel)

    private val newResource: Channel<ResourceAction> = Channel<ResourceAction>()

    val releaseResource: Channel<ResourceAction> = Channel<ResourceAction>()
    companion object {
        fun create(id: String): DoormanClient {
            val c = DoormanClient(id)
            return c
        }

    }

    init {

        CoroutineScope(Dispatchers.Unconfined + job).launch(CoroutineName("ClientRunLoop")) { run() }
    }
    fun getMaster(): String = "$master:$port"

    suspend fun requestResource(id: String, capacity: Double): Resource {
        println("[Client ${this.id}][Request Resource] Requesting resource $id with capacity $capacity")
        return this.requestResourceWithPriority(id,capacity,0)
    }

    suspend fun requestResourceWithPriority(id:String, capacity: Double, priority:Long): Resource {
        val resource = Resource(id = id, client = this, wants = capacity, priority = priority)
        val errC: Channel<Throwable?> = Channel(onUndeliveredElement = { e -> println("[ERROR] Error in errC: $e") })

        val newResourceAction = ResourceAction(resource,errC)

        newResource.send(newResourceAction)
        println("[Client ${this.id}][Request Resource][ts: ${System.currentTimeMillis()}] Sent request for resource $id with capacity $capacity, now waiting for res creation errorChan")
        val errCRes = errC.receive()
        println("[Client][Request Resource][ts: ${System.currentTimeMillis()}] Received errC, request for resource $id with capacity $capacity")
        if(errCRes == null) return resource else throw Exception("Error creating resource $id with capacity $capacity: $errCRes")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun close() {
        if(newResource.isClosedForSend.not()) newResource.close()
        if(releaseResource.isClosedForSend.not()) releaseResource.close()
        // Release all held resources
        val releaseCapacityReq = Doorman.ReleaseCapacityRequest.newBuilder().setClientId(this.id)
        for(r in resources.values) {
            releaseCapacityReq.addResourceId(r.id)
            r.capacity.close()
        }
        if(channel.isShutdown.not()) {
            channel.shutdown()
        }
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
        val discoverResponse = rpcClient.discovery(discoveryRequest)
        return discoverResponse
    }


    // run is the client's main loop. It takes care of requesting new
    // resources, and managing ones already claimed. This is the only
    // method that should be modifying the client's internal state and
    // performing RPCs.
    @OptIn(ExperimentalCoroutinesApi::class, ObsoleteCoroutinesApi::class)
    private suspend fun run() {
        var retryCount = 0
         var wakeUp = ticker(1000)
        try {
            println("[Client ${this.id}] Starting run loop")
            while (true) {
                select {
                    newResource.onReceive { rA ->
                        try {
                            println("[Client ${this@DoormanClient.id}] [New Resource] Req to create resource $rA")
                            val resourceAction = rA // .getOrNull()
                            if (resourceAction == null) {
                                println("[Client ${this@DoormanClient.id}] Error creating resource: ResourceAction is null!!!")
                            } else {
                                val err = addResource(resourceAction.resource)
                                if (err != null) {
                                    scope.launch {
                                        println("[Client ${this@DoormanClient.id}] Error adding resource: $err")
                                        resourceAction.errC.send(err)
                                    }
                                } else {
                                    scope.launch{ resourceAction.errC.send(null) }
                                }
                            }
                        } catch (e: Exception) {
                            rA.errC.send(e)
                        }
                    }

                    releaseResource.onReceive { rA ->
                        try {
                            println("[Client ${this@DoormanClient.id}] [Release Resource] Req to release resource $rA")
                            val resourceAction: ResourceAction = rA

                            val err = removeResource(resourceAction.resource)
                            if (err != null) {
                                scope.launch {
                                    println("[Client ${this@DoormanClient.id}] Error removing resource: $err")
                                    resourceAction.errC.send(err)
                                }
                            } else {
                                scope.launch{ resourceAction.errC.send(null) }
                            }

                        } catch (e: Exception) {
                            rA.errC.send(e)
                        }
                    }

                    wakeUp.onReceiveCatching {
                        it.getOrNull() ?: println("[Client ${this@DoormanClient.id}] Got null in waking up channel instead of boolean!")
                        return@onReceiveCatching 0
                    }
                }


                var interval : Long
                val (newInterval, newRetryCount) = if(resources.isEmpty().not()) {
                    performRequests(retryCount)
                } else {
                    // println("[Client ${this.id}] No resources, skipped performing requests, sleeping for 1 second")
                    Pair(MIN_REFRESH_INTERVAL.toLong(),retryCount)
                }
                interval = newInterval
                retryCount = newRetryCount

                wakeUp.cancel()
                wakeUp = ticker(interval)

                // println("[Client ${this.id}] Looping run() at ${System.currentTimeMillis()}")
            }
        } catch (e: Exception) {
            println("[Client ${this@DoormanClient.id}] Error in run: $e , stacktrace: ${e.stackTraceToString()}")
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
            println("[Remove Resource] Releasing capacity $releaseCapacityReq")
            val response = rpcClient.releaseCapacity(releaseCapacityReq)
            println("Release Capacity Response: ${response.allFields}")
            if(response.mastership.masterAddress == null) {
                return MasterUnknownError("Master address is null in response for request $releaseCapacityReq")
            }
            if(response.mastership.masterAddress.isNullOrEmpty().not() && response.mastership.masterAddress != getMaster()) {
                refreshMaster()
                // retry release
                val retryResponse = rpcClient.releaseCapacity(releaseCapacityReq)
                if (retryResponse.mastership.masterAddress != getMaster() || retryResponse.mastership.masterAddress == null) {
                    return Exception("Master address mismatch after retry, expected ${getMaster()} but rpc says master is $retryResponse")
                }
            }
        } catch (e: Exception) {
            // We can ignore this error, as the resource will be released eventually when we do not renew the leave or the server forgets about it when masters change.
            println("[Remove Resource] Error releasing capacity: $e")
            return null
        }

        return null
    }



    /*
     * performRequests performs the actual RPCs to the server. It returns the interval in Milli and new retryCount
     */
    private fun performRequests(retryCount: Int): Pair<Long, Int> {

        // Create a get capacity request in bulk for all resources to reduce QPS to doorman servers
        val getCapacityReq = Doorman.GetCapacityRequest.newBuilder().setClientId(this.id)

        for(r in resources.values) {
            val resourceReq = Doorman.ResourceRequest
                .newBuilder()
                .setResourceId(r.id)
                .setPriority(r.priority)
                .setWants(r.wants)


            if(r.lease != null) {
                resourceReq.setHas(r.lease!!)
            }

            getCapacityReq.addResource(
                resourceReq.build()
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

        // println("[$id : Perform Requests] [ts:${System.currentTimeMillis()}] Capacity Response: ${capacityResponse.responseList.get(0).gets.capacity}")

        // update client state with the response capacity and lease;
        for(r in capacityResponse.responseList) {
            val clientResource = resources[r.resourceId]
            if (clientResource == null){
                println("ERROR [$id : Perform Requests] Resource ${r.resourceId} not found in client resources")
                continue
            }
            val oldCapacity:Double = clientResource.lease?.capacity ?: -1.0

            // update lease
            clientResource.lease = r.gets

            // Only send a message down the channel if the capacity has changed.
            if(oldCapacity != r.gets.capacity) {
                scope.launch {
                    clientResource.capacity.send(r.gets.capacity)
                    println("[$id : Perform Requests] Capacity update sent for ${r.resourceId} , $oldCapacity -> ${r.gets.capacity}")
                }
            } else {
                //println("[$id : Perform Requests] Capacity for ${r.resourceId} has not changed, not sending update")
            }
        }

        // Figure out refresh interval, find the minimum refresh interval
        var interval = 15.minutes // some long duration which should get overwritten
        for( r in capacityResponse.responseList) {
            val refresh = r?.gets?.refreshInterval?.seconds ?: 0.milliseconds
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


