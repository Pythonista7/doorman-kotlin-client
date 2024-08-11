package client

import doorman.Doorman
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex

class Resource(
    override val id: String,
    wants: Double = 0.0,
    priority: Long = 0,
) : IResource {
    private val mutex = Mutex()

    override var wants: Double = wants

    override val capacity: Channel<Double> = Channel()

    override val priority: Long = priority

    override var lease: Doorman.Lease? = null

    override suspend fun ask(requestedWants: Double): Throwable? {
        if (requestedWants < 0) {
            return IllegalArgumentException("Wants must be positive")
        }
        this.mutex.lock(owner = this.id)
        this.wants = requestedWants
        this.mutex.unlock(owner = this.id)
        return null
    }

    override suspend fun release(): Throwable? {
        val errorChan = Channel<Throwable?>()
        DoormanClient.releaseResource.send(
            ResourceAction(resource = this@Resource ,errC = errorChan)
        )
        return errorChan.receiveCatching().getOrNull()
    }

    override suspend fun expiry(): Long = this.lease?.expiryTime ?: 0
}
