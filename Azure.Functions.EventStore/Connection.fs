namespace Azure.Functions.EventStore

open EventStore.Operations

module Connection =


    let create    :  Create    = fun request    -> async { return Error "not implemented" }
    let terminate :  Terminate = fun connection -> async { return Error "not implemented" }
     
    let readStreamEventsBackwardAsync : ReadStreamEventsBackward =
    
        fun stream startIndex count -> async { return Error "not implemented" }

    let appendToStream : AppendToStream =

        fun stream event -> async { return Error "not implemented" }