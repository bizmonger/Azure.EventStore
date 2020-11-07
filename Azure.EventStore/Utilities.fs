namespace EventStore

open Azure.Table
open EventStore.Core.Language
open Azure.Entities

module Utilities =

    let valueFromRowKey       (RowKey       v) = v
    let valueFromPartitionKey (PartitionKey v) = v
    let valueFromJson         (JSON         v) = v
    let valueFromData         (Data         v) = valueFromJson v
    let valueFromMeta         (MetaData     v) = v
    let valueFromEventType    (EventType    v) = v
    let valueFromStreamId     (Stream       v) = v


    let toEvent(v:EventEntity) : Event = {
        Id        = v.RowKey
        Stream    = v.Stream    |> Stream
        Data      = v.Data      |> JSON |> Data
        MetaData  = v.MetaData  |> MetaData
        EventType = v.EventType |> EventType
        Timestamp = v.Timestamp.Date
    }