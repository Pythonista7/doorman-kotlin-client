package client;

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import newQPS
import org.junit.jupiter.api.Test;

public class Simulation {

    val apiClient = HttpClient(CIO)

    @Test
    fun `test_ratelimiter_with_real_resource`() =runBlocking {
        val client = DoormanClient.create("test-client")
        println("Client created: $client")

        val r = client.requestResource("test-resource", 10.0)
        println("Resource created: test-resource")
        println("Client resources: ${client.resources.values}")
        delay(3000)
        println("Resource Lease = ${r.lease}")


        val err = r.release()
        println("Resource released: $err")
        println("Client resources: ${client.resources}")
        client.close()
    }

    @Test
    fun test_simulation() = runBlocking {
        /*
        This assumes that there is a resource as defined below in the doorman server
          - identifier_glob: fair
            capacity: 1000
            safe_capacity: 10
            description: fair share example
            algorithm:
              kind: FAIR_SHARE
              lease_length: 60
              refresh_interval: 5 // keeping this small for testing purposes.
         */
        val small1Client = DoormanClient.create("small1Client-client")
        val small1Resource = small1Client.requestResource("proportional",20.0)
        val small1RL = newQPS(small1Resource)

        val small2Client = DoormanClient.create("small2Client-client")
        val small2Resource = small2Client.requestResource("proportional",20.0)
        val small2RL = newQPS(small2Resource)

        val small3Client = DoormanClient.create("small3Client-client")
        val small3Resource = small3Client.requestResource("proportional",20.0)
        val small3RL = newQPS(small3Resource)

//        val small4Client = DoormanClient.create("small4Client-client")
//        val small4Resource = small4Client.requestResource("proportional",40.0)
//        val small4RL = newQPS(small4Resource)
//
//        val bigClient = DoormanClient.create("bigClient-client")
//        val bigResource = bigClient.requestResource("proportional",60.0)
//        val bigRL = newQPS(bigResource)
//
//        val biggerClient = DoormanClient.create("biggerClient-client")
//        val biggerResource = biggerClient.requestResource("fair", 2000.0)
//        val biggerRL = newQPS(biggerResource)

        val clients = listOf(small1Client, small2Client, small3Client) //, small4Client, bigClient) //, biggerClient)
        val rateLimiters = listOf(small1RL, small2RL, small3RL)// , small4RL, bigRL)//, biggerRL)


        val job = SupervisorJob()
        val scope = CoroutineScope(Dispatchers.IO + job + CoroutineName("ApiCallSupervisor"))
        val jobs = rateLimiters.map { rlm ->
            scope.launch(CoroutineName("ApiCall-${rlm.resourceId}-${rlm.hashCode()}")) {
                while(true) {
                     println("[API][RM:${rlm.resourceId} ${this.hashCode()}] Entering wait...")
                     rlm.wait()
                     println("[API][RM:${rlm.resourceId} ${this.hashCode()}] Making request to http://localhost:8080/resource/${rlm.resourceId}")
                    try {
                        val r = apiClient.get("http://localhost:8080/resource/${rlm.resourceId}")
                        println("[API][RM:${rlm.resourceId}] Response: $r")
                    } catch (e: Exception) {
                        println("[API][RM:${rlm.resourceId}] Error making request: ${e.message}")
                    }
                }
            }
        }

        while(true) {
            clients.random().let {
                var oldCapacity = 0.0
                it.resources.firstNotNullOfOrNull {
                    r -> {
                        oldCapacity = r.value.lease?.capacity ?: 0.0
                        runBlocking { r.value.release() }
                    }
                }
                if (oldCapacity > 0) {
                    var r = it.requestResource("proportional", 0.0)
                } else {
                    var r = it.requestResource("proportional", 10.0)
                }
            }
            delay(20000)
        }

//        while(true) {
//
//            jobs.forEach {
//                println("Job: ${it.job} active: ${it.isActive} completed: ${it.isCompleted}, cancelled: ${it.isCancelled}")
//            }
//
//            rateLimiters.forEach {
//                println("RateLimiter: ${it.resourceId} currentInterval: ${it.currentInterval()} isActive: ${it.isActive()}")
//            }
//
////            println("Initially...")
////            println("New Capacity of small1: ${small1Resource.lease?.capacity}")
////            println("New Capacity of small2: ${small2Resource.lease?.capacity}")
////            println("New Capacity of small3: ${small3Resource.lease?.capacity}")
////            println("New Capacity of small4: ${small4Resource.lease?.capacity}")
////            println("New Capacity of big: ${bigResource.lease?.capacity}")
////            println("New Capacity of bigger: ${biggerResource.lease?.capacity}")
//
//            delay(5000)
//
////            println("After 5 seconds...")
////            println("New Capacity of small1: ${small1Resource.lease?.capacity}")
////            println("New Capacity of small2: ${small2Resource.lease?.capacity}")
////            println("New Capacity of small3: ${small3Resource.lease?.capacity}")
////            println("New Capacity of small4: ${small4Resource.lease?.capacity}")
////            println("New Capacity of big: ${bigResource.lease?.capacity}")
////            println("New Capacity of bigger: ${biggerResource.lease?.capacity}")
//
//
//            // Release a random resource and request a new capacity
//            clients.asSequence().shuffled().first().let { client->
//                client.resources.values.first().let { resource ->
//                    println("[RELEASE] Releasing resource ${resource.id}")
//                    val existingCapacity = resource.lease?.capacity
//                    val err = resource.release()
//                    delay(1000)
//                    if (err != null) {
//                        println("[RELEASE] Error releasing resource ${resource.id} : $err")
//                    } else {
//                        println("[RELEASE] Released resource ${resource.id}")
//                        val newCapacity =
//                            (Math.random() * 100 * listOf(1, -1).shuffled().first() + (existingCapacity ?: 0.0)).coerceAtLeast(
//                                1.0
//                            )
//                        println("[REQUEST] Requesting new capacity for ${client.id} from $existingCapacity to $newCapacity")
//                        val r = client.requestResource(
//                            "fair",
//                            newCapacity
//                        )
//                        println("[REQUEST] Requested new capacity for ${client.id} from $existingCapacity to $newCapacity")
//                    }
//                }
//            }
//            delay(5000)
//        }

        jobs.joinAll()
        job.join()

    }


}
