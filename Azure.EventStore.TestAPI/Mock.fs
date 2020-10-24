namespace Azure.EventStore.TestAPI

open EventStore.Core.Language
open EventStore.Language

module Mock =

    let someEndpoint    = "some_endpoint"
    let someCrecdential = { Username= "some_username"; Password= "some_password"}

    let someConnectionRequest : ConnectionRequest = {
        ConnectionString   = someEndpoint
        Credential         = someCrecdential
    }

    let someStream = EventStore.Core.Language.Stream "some_stream"
    let someEvent : EventStore.Core.Language.Event = {
        Id        = "some_event_id"
        Data      = Data      <| JSON ""
        MetaData  = MetaData  <| ""
        EventType = EventType <| ""
    }