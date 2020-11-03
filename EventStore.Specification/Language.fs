namespace EventStore

open EventStore.Core.Language

module Language =

    type AsyncResult<'a,'e> = Async<Result<'a,'e>>
    type ErrorDescription   = string
    
    type Credential = { Username : string; Password : string }

    type StartIndex = int
    type Count      = int

    type ConnectionString = string

    type Connection = {
        Context          : obj
        ConnectionString : ConnectionString
    }

    type ConnectionRequest = {
        ConnectionString : ConnectionString
        Credential       : Credential
    }