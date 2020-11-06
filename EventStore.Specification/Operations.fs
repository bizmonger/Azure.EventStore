namespace EventStore

open Language

module Operations =

    type Create    = ConnectionRequest -> AsyncResult<Connection , ErrorDescription>
    type Terminate = Connection        -> AsyncResult<unit       , ErrorDescription>