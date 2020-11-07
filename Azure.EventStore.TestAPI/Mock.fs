namespace Azure.EventStore.TestAPI

open System
open EventStore.Core.Language
open EventStore.Language
open Azure.Table

module Mock =

    let SomeStreamTable      = Table "Stream"
    let someConnectionString = "UseDevelopmentStorage=true"
    let someCrecdential      = { Username= "some_username"; Password= "some_password"}

    let someConnectionRequest : ConnectionRequest = {
        ConnectionString   = someConnectionString
        Credential         = someCrecdential
    }

    let someEvent  : EventStore.Core.Language.Event = {
        Id        = "some_event_id"
        Stream    = "some_stream"    |> Stream
        EventType = "some_eventType" |> EventType
        MetaData  = "some_meta_data" |> MetaData
        Data      = "some json"      |> JSON |> Data
        Timestamp = DateTime.Now
    }

    let someEvent2 = { someEvent with Id = "some_event_id_2"; Data="some json 2" |> JSON |> Data }
    let someEvent3 = { someEvent with Id = "some_event_id_3"; Data="some json 3" |> JSON |> Data }
    let someEvent4 = { someEvent with Id = "some_event_id_4"; Data="some json 4" |> JSON |> Data }

    let someStream = someEvent.Stream