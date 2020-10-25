namespace Azure

open Microsoft.WindowsAzure.Storage.Table

type StreamEntity(name:string) =

    inherit TableEntity()

type EventEntity(name:string) =

    inherit TableEntity()