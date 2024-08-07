package org.example.examples

import doorman.CapacityGrpc
import doorman.Doorman
import io.grpc.ManagedChannelBuilder
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

fun main() {
    val channel =
        ManagedChannelBuilder
            .forAddress("localhost", 15000)
            .usePlaintext()
            .build()
    val stub = CapacityGrpc.newBlockingStub(channel)

    val discoveryReq = Doorman.DiscoveryRequest.newBuilder().build()
    val discoveryResp = stub.discovery(discoveryReq)
    println(discoveryResp)

    val allocations =
        listOf(
            Doorman.ResourceRequest
                .newBuilder()
                .setResourceId("proportional")
                .setPriority(0)
                .setWants(40.0)
                .build(),
            Doorman.ResourceRequest
                .newBuilder()
                .setResourceId("proportional")
                .setPriority(0)
                .setWants(40.0)
                .build(),
            Doorman.ResourceRequest
                .newBuilder()
                .setResourceId("proportional")
                .setPriority(0)
                .setWants(500.0)
                .build(),
        )

    allocations.forEachIndexed { idx,al ->
        val res = stub.getCapacity(
            Doorman.GetCapacityRequest.newBuilder()
                .setClientId("client$idx")
                .addResource(al)
                .build(),
        )
        val formattedResponse = res.responseList.map {
            val expiryTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(it.gets.expiryTime), ZoneId.systemDefault())
            val formattedExpiryTime = expiryTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            "${it.resourceId} was allocated ${it.gets.capacity} which expires at $formattedExpiryTime (in ${it.gets.expiryTime - Instant.now().epochSecond} seconds)"
        }
        println("Req: $al, \n Res:$res \n $formattedResponse")
    }
}
