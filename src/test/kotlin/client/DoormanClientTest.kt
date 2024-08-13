package client

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test

class DoormanClientTest {

    @Test
    fun `clientInitTest`(){
        val client = DoormanClient.create("test-client")
        println("Client created: $client")
        client.close()
    }

    @Test
    fun `resourceInitTest`(){
        val resource = Resource("test-resource")
        println("Resource created: $resource")
    }

    @Test
    fun `addResourceWithClientTest`(): Unit = runBlocking(CoroutineName("ash-test-1")) {
        val client = DoormanClient.create("test-client")
        println("Client created: $client")
        client.requestResource("test-resource", 10.0)
        println("Resource created: test-resource")
        client.close()
    }



    @Test
    fun `test with actual doorman resource`(): Unit = runBlocking(CoroutineName("ash-test-3")) {
        val client = DoormanClient.create("apple-client")
        val fResourceApples = client.requestResource("proportional",10.0)
        println("Resource created: $fResourceApples")
        val appleRateLimiter = RateLimiter(fResourceApples)
        println("Rate limiter created: $appleRateLimiter")
        // define a coroutine context to kill the rate limiter after 3 seconds
        CoroutineScope(Dispatchers.Default).launch {
            delay(5000)
            println("Closing the rate limiter and client")
            appleRateLimiter.close()
            client.close()
        }

        println("Now Rate limiting... ")
        val start = System.currentTimeMillis()
        println("Start: $start")
        val result = appleRateLimiter.wait()
        println("Wait: $result , Time taken: ${(System.currentTimeMillis() - start)/1000.00} seconds") // Should be around 100ms as we are waiting to manage a rate of 10 RPS
        assert(System.currentTimeMillis() - start < 250)
    }


    @Test
    fun `test capacity allocation algo "proportional"`() = runBlocking(CoroutineName("ash-test-4"))  {
        /*
        This assumes that there is a resource as defined below in the doorman server
        resources:
          - identifier_glob: proportional
            capacity: 100
            safe_capacity: 10
            description: proportional example
            algorithm:
              kind: PROPORTIONAL_SHARE
              lease_length: 60
              refresh_interval: 10
         */

        val orangeClient = DoormanClient.create("orange-client")
        val fResourceOranges = orangeClient.requestResource("proportional",20.0)
        println("fResourceOranges Resource created: $fResourceOranges")

        val appleClient = DoormanClient.create("apple-client")
        val fResourceApples = appleClient.requestResource("proportional",20.0)
        println("fResourceApples Resource created: $fResourceApples")

        val iceClient = DoormanClient.create("ice-client")
        val fResourceIce = iceClient.requestResource("proportional",20.0)
        println("fResourceIce Resource created: $fResourceIce")

        // At this point the 3 clients should have consumed 60 capacity out of 100

        // Now lets demand more than what's available
        val bananaClient = DoormanClient.create("banana-client")
        val fResourceBananas = bananaClient.requestResource("proportional",60.0)
        println("fResourceBananas Resource created: $fResourceBananas")

        delay(10000) // wait for 10-15 seconds to let the capacity refresh and re-balance

        // Now lets examine the capacity
        println("New Capacity of oranges: ${fResourceOranges.lease?.capacity}")
        println("New Capacity of apples: ${fResourceApples.lease?.capacity}")
        println("New Capacity of ice: ${fResourceIce.lease?.capacity}")
        println("New Capacity of bananas: ${fResourceBananas.lease?.capacity}")
        /*
        Expecting to see:
        New Capacity of oranges: 20.0
        New Capacity of apples: 20.0
        New Capacity of ice: 20.0
        New Capacity of bananas: 30.0
         */
    }

    @Test
    fun `test capacity allocation algo "fair"`() = runBlocking(CoroutineName("ash-test-5"))  {
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
        val small1Resource = small1Client.requestResource("fair",100.0)

        val small2Client = DoormanClient.create("small2Client-client")
        val small2Resource = small2Client.requestResource("fair",100.0)

        val small3Client = DoormanClient.create("small3Client-client")
        val small3Resource = small3Client.requestResource("fair",100.0)

        val small4Client = DoormanClient.create("small4Client-client")
        val small4Resource = small4Client.requestResource("fair",100.0)

        val bigClient = DoormanClient.create("bigClient-client")
        val bigResource = bigClient.requestResource("fair",1000.0)

        delay(5000)

        val biggerClient = DoormanClient.create("biggerClient-client")
        val biggerResource = biggerClient.requestResource("fair",2000.0)

        println("Initially...")
        println("New Capacity of small1: ${small1Resource.lease?.capacity}")
        println("New Capacity of small2: ${small2Resource.lease?.capacity}")
        println("New Capacity of small3: ${small3Resource.lease?.capacity}")
        println("New Capacity of small4: ${small4Resource.lease?.capacity}")
        println("New Capacity of big: ${bigResource.lease?.capacity}")
        println("New Capacity of bigger: ${biggerResource.lease?.capacity}")

        delay(8000)

        println("After 8 seconds...")
        println("New Capacity of small1: ${small1Resource.lease?.capacity}")
        println("New Capacity of small2: ${small2Resource.lease?.capacity}")
        println("New Capacity of small3: ${small3Resource.lease?.capacity}")
        println("New Capacity of small4: ${small4Resource.lease?.capacity}")
        println("New Capacity of big: ${bigResource.lease?.capacity}")
        println("New Capacity of bigger: ${biggerResource.lease?.capacity}")

    }

}