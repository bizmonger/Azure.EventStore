namespace EventStore

open EventStore.Core.Language

module Language =

    type AsyncResult<'a,'e> = Async<Result<'a,'e>>
    type ErrorDescription   = string
    
    type Credential = { Username : string; Password : string }

    type StartIndex = int64
    type Count      = int

    type ConnectionString = string

    type Connection = {
        Context                  : obj
        ConnectionString         : ConnectionString
        AppendToStreamAsync      : Stream -> Event               -> AsyncResult<EventId   , ErrorDescription>
        ReadStreamEventsBackward : Stream -> StartIndex -> Count -> AsyncResult<Event seq , ErrorDescription>
        Terminate                : Connection                    -> AsyncResult<unit      , ErrorDescription>
    }

    type ConnectionRequest = {
        ConnectionString : ConnectionString
        Credential       : Credential
    }