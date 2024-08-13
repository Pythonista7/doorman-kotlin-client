package client

import doorman.Doorman
import kotlinx.coroutines.channels.Channel

interface IResource {
    val id: String
    val client: DoormanClient?
    val wants: Double
    val capacity: Channel<Double>

    val priority: Long

    var lease: Doorman.Lease?

    // val client: DoormanClient
    suspend fun ask(capacity: Double): Throwable?
    suspend fun release(): Throwable?

    suspend fun expiry(): Long
}

