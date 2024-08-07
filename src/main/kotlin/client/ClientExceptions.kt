package client

class DuplicateResourceError(s: String) : Throwable()

class MasterUnknownError(s: String) : Throwable()