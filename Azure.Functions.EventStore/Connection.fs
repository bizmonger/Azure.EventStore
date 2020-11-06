namespace Azure.Functions.EventStore

open EventStore.Operations

module Connection =

    let create    : Create    = fun request    -> async { return Error "not implemented" }
    let terminate : Terminate = fun connection -> async { return Error "not implemented" }