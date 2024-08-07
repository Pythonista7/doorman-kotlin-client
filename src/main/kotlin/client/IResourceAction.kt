package client

import kotlinx.coroutines.channels.Channel

interface IResourceAction {
    val resource: IResource
    val errC: Channel<Throwable>
}