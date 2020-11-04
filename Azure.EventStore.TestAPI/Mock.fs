namespace Azure.EventStore.TestAPI

open System
open EventStore.Core.Language
open EventStore.Language

module Mock =

    let someConnectionString = "UseDevelopmentStorage=true"
    let someCrecdential      = { Username= "some_username"; Password= "some_password"}

    let someConnectionRequest : ConnectionRequest = {
        ConnectionString   = someConnectionString
        Credential         = someCrecdential
    }

    let someStream = EventStore.Core.Language.Stream "someStream"

    let someEvent  : EventStore.Core.Language.Event = {
        Id        = "some_event_id"
        EventType = "some_eventType" |> EventType
        Data      = "some json"      |> JSON |> Data
        Timestamp = DateTime.Now
    }