namespace Azure

open Microsoft.WindowsAzure.Storage.Table

type StreamEntity(name:string) =

    inherit TableEntity()

type EventEntity(name:string) =

    inherit TableEntity()

    member val Stream    = "" with get,set
    member val Data      = "" with get,set
    member val MetaData  = "" with get,set
    member val EventType = "" with get,set