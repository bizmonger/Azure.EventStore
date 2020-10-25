namespace EventStore

open Language
open EventStore.Core.Language

module Operations =

    type Create    = ConnectionRequest -> AsyncResult<Connection , ErrorDescription>
    type Terminate = Connection        -> AsyncResult<unit       , ErrorDescription>

    type AppendToStream           = Stream -> Event               -> ConnectionString -> AsyncResult<unit      , ErrorDescription>
    type ReadStreamEventsBackward = Stream -> StartIndex -> Count -> ConnectionString -> AsyncResult<Event seq , ErrorDescription>