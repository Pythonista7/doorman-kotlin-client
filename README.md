# Kotlin Doorman Client

This is a Kotlin client for the [Doorman](https://github.com/Pythonista7/doorman) service which allows voluntary client-side rate limiting. The big idea can be captured in this image:

![Doorman](https://github.com/Pythonista7/doorman/blob/master/doc/loadtest/overview.png)

This implementation is based on the [Go Client](https://github.com/Pythonista7/doorman/blob/master/go/client/doorman/client.go) from the Doorman project, hence the bugs/limitation of the Go client are also present in this implementation at the moment.


Check out the tests and the [Doorman Readme](https://github.com/Pythonista7/doorman/blob/master/README.md) for more.

## Overview

### Client
A client for the Doorman service which allows voluntary client-side rate limiting. It keeps track of resources and runs a background goroutine to periodically update the server with the current state of the client based on refresh intervals.

### Resource
A resource is a unique identifier for a rate-limited entity. It is used to track the rate limit for a specific entity and the applied rate limit for the resource is dynamically updated by the client based on the server's response.

### RateLimit
A wrapper class that accepts a resource and applies the rate limit to it. It is used to control the rate limit for accessing a specific resource , by calling the `wait()` method before accessing the resource.

## Example

#### Basic Usage
```kotlin
runBlocking {
    val client = DoormanClient.create("example-client")
    val resourceApples = client.requestResource("apples",10.0)
    val rateLimit = RateLimiter(resourceApples)

    val endTime = System.currentTimeMillis() + 7000

    var count = 0

    // Tracker coroutine to keep track of the requests made every second
    val scope = CoroutineScope(this.coroutineContext).launch {
        while (true) {
            delay(1000)
            println("[Tracker] Total requests made: $count at ${ZonedDateTime.now()}")
        }
    }

    while(System.currentTimeMillis() < endTime) {
        rateLimit.wait()
        count ++
        // current time in human-readable form
        println("Accessing resource `Apples` at ${ZonedDateTime.now()}")
    }

    scope.cancelAndJoin() // close the tracker coroutine.
}
```

#### Where it shines
```kotlin

runBlocking {
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
```

#### Output
```text
Initially...
New Capacity of small1: 100.0
New Capacity of small2: 100.0
New Capacity of small3: 100.0
New Capacity of small4: 100.0
New Capacity of big: 600.0
New Capacity of bigger: 0.0

...
...

After 8 seconds...
New Capacity of small1: 100.0
New Capacity of small2: 100.0
New Capacity of small3: 100.0
New Capacity of small4: 100.0
New Capacity of big: 300.0
New Capacity of bigger: 300.0
```