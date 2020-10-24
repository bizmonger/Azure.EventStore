namespace EventStore

open Event.Language

module Language =

    type AsyncResult<'a,'e> = Async<Result<'a,'e>>
    type ErrorDescription   = string
    
    type Credential = { Username : string; Password : string }

    type StartIndex = int64
    type Count      = int

    type Connection = {
        AppendToStreamAsync      : Stream -> Event -> AsyncResult<unit, ErrorDescription>
        ReadStreamEventsBackward : Stream -> StartIndex -> Count -> AsyncResult<Event seq, ErrorDescription>
    }

    type Endpoint = string

    type ConnectionRequest = {
        Endpoint   : Endpoint
        Credential : Credential
    }