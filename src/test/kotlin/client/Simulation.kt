package client;

import QpsRateLimiter
import doorman.Doorman
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import kotlinx.coroutines.*
import newQPS
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.*
import java.time.ZonedDateTime
import org.mockito.junit.jupiter.MockitoExtension
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Timer
import kotlin.concurrent.timerTask

@ExtendWith(MockitoExtension::class)
class Simulation {

    private val apiClient = HttpClient(CIO)

    @Mock
    lateinit var mockDoormanClient: DoormanClient
    @Test
    fun `test_ratelimiter_init`() =runBlocking {

        // mock the get capacity request
        val mockResource = mock(Resource::class.java)
        `when`(mockDoormanClient.requestResource("test-resource", 10.0)).thenReturn(mockResource)
        `when`(mockResource.lease).thenReturn(
            Doorman.Lease.newBuilder()
                .setCapacity(10.0)
                .setExpiryTime(ZonedDateTime.now().plusMinutes(10).toInstant().toEpochMilli())
                .setRefreshInterval(10)
                .build()
        )
        // do nothing when close is called
        doNothing().`when`(mockDoormanClient).close()


        // val client = DoormanClient.create("test-client")
        println("Client created")

        val r = mockDoormanClient.requestResource("test-resource", 10.0)
        println("Resource created: test-resource")
        // println("Client resources: ${client.resources.values}")
        delay(3000)
        println("Resource Lease = ${r.lease}")

        val err = r.release()
        println("Resource released: $err")
        // println("Client resources: ${client.resources}")
        mockDoormanClient.close()
    }


    fun convertEpochToDateTimeString(epoch: Long, zoneId: ZoneId = ZoneId.systemDefault()): String {
        // Convert epoch to Instant
        val instant = Instant.ofEpochSecond(epoch)

        // Convert Instant to LocalDateTime
        val dateTime = LocalDateTime.ofInstant(instant, zoneId)

        // Define a formatter
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        // Format LocalDateTime to string
        return dateTime.format(formatter)
    }

    @Test
    fun simulationOne(): Unit = runBlocking {

        val clients = mutableMapOf<String,DoormanClient>()
        val rateLimiters = mutableMapOf<String,QpsRateLimiter>()

        suspend fun simulateClient(clientId: String, resourceIds: List<Pair<String,Double>>) {
            val client = DoormanClient.create(clientId)
            clients[clientId] = client
            println("Client $clientId created")
            resourceIds.forEach { (resourceId,capacity) ->
                val resource = client.requestResource(resourceId, capacity)
                println("Resource ${resource.id} created for capacity ${resource.lease?.capacity} expiring at ${convertEpochToDateTimeString(resource.lease?.expiryTime?:0)}")
                rateLimiters[clientId+"_"+resourceId] = newQPS(resource)
                println("RateLimiter created for resource $resourceId")
            }
        }

        fun logResourcesAndRateLimiters() {
            clients.values.map {
                    client ->
                println("------------------------------------------------")
                println("Client ${client.id} :")
                client.resources.map {
                        resource ->
                    println("Resource ${resource.value.id} for client ${client.id} has capacity ${resource.value.lease?.capacity} expiring at ${convertEpochToDateTimeString(resource.value.lease?.expiryTime?:0)}")
                    println("Rate-limiter for resource ${resource.value.id} is active: ${rateLimiters[client.id+"_"+resource.value.id]?.isActive()} and current interval is ${rateLimiters[client.id+"_"+resource.value.id]?.currentInterval()}")
                }
            }
            println("------------------------------------------------")
        }

        // Assumed doorman configuration
        /*
          - identifier_glob: tf
            capacity: 100
            safe_capacity: 10
            description: fair share example
            algorithm:
              kind: FAIR_SHARE
              lease_length: 60
              refresh_interval: 30
         */

        val loggerTask = timerTask {
            println("================================================")
            println("Logging resources and rate limiters")
            logResourcesAndRateLimiters()
            println("================================================")
        }
        Timer().schedule(loggerTask,5000,5000)


        // INITIAL STATES
        simulateClient("client1", listOf("tf" to 100.0))

        simulateClient("client2", listOf("tf" to 100.0))

        simulateClient("client3", listOf("tf" to 30.0))

        logResourcesAndRateLimiters()


        val supervisorJob = SupervisorJob()
        val scope = CoroutineScope(Dispatchers.IO + supervisorJob)
        val jobs = rateLimiters.values.map { rlm ->
            println("Creating job for rate limiter ${rlm.resourceId}")
            scope.launch(CoroutineName("ApiCall-${rlm.resourceId}-${rlm.hashCode()}")) {
                while(true) {
                    // println("[API][RM:${rlm.resourceId} ${this.hashCode()}] Entering wait...")
                    rlm.wait()
                    // println("[API][RM:${rlm.resourceId} ${this.hashCode()}] Making request to http://localhost:8080/resource/${rlm.resourceId}")
                    try {
                        val r = apiClient.get("http://localhost:8080/resource/${rlm.resourceId}")
                        // println("[API][RM:${rlm.resourceId}] Response: $r")
                    } catch (e: Exception) {
                        println("[API][RM:${rlm.resourceId}] Error making request: ${e.message}")
                    }
                }
            }
        }

        println("Waiting for jobs to complete.....")

        val jobsLoggerTask = timerTask {
            println("================================================")
            println("Logging Job Tasks")
            println("Supervisor: ${supervisorJob.children} , isActive: ${supervisorJob.isActive} , isCompleted: ${supervisorJob.isCompleted} , isCancelled: ${supervisorJob.isCancelled}")

            jobs.forEach {
                println("Job: ${it.job} active: ${it.isActive} completed: ${it.isCompleted}, cancelled: ${it.isCancelled}")
            }
            println("================================================")
        }
        Timer().schedule(jobsLoggerTask,10000,10000)

        jobs.joinAll()
    }

}
