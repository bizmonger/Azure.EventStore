namespace EventStore

open Language
open Event.Language

module Operations =

    type Create    = ConnectionRequest -> AsyncResult<Connection , ErrorDescription>
    type Terminate = Connection        -> AsyncResult<unit       , ErrorDescription>

    type AppendToStream           = Stream -> Event               -> AsyncResult<unit      , ErrorDescription>
    type ReadStreamEventsBackward = Stream -> StartIndex -> Count -> AsyncResult<Event seq , ErrorDescription>