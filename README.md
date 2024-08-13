# Kotlin Doorman Client

This is a Kotlin client for the [Doorman](https://github.com/Pythonista7/doorman) service which allows voluntary client-side rate limiting.
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
See more in `DoormanClientTest.kt`
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