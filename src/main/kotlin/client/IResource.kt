package client

import doorman.Doorman
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.sync.Mutex

interface IResource {
    val id: String
    val wants: Double
    val capacity: ReceiveChannel<Double>

    val lease: Doorman.Lease?

    val client: DoormanClient
    suspend fun ask(capacity: Double): Throwable?
    suspend fun release(): Throwable?

    suspend fun expiry(): Long
}

