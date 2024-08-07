package client

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

interface Resource {
    val id: String
    val wants: Double
    val capacity: ReceiveChannel<Double>
    suspend fun ask(capacity: Double): Result<Unit>
    suspend fun release(): Result<Unit>
}

interface ResourceAction {
    val resource: Resource
    val errC: Channel<Throwable>
}