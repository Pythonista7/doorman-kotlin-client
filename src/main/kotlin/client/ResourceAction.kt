package client

import kotlinx.coroutines.channels.Channel

data class ResourceAction(override val resource: IResource, override val errC: Channel<Throwable?>) : IResourceAction
