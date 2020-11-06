namespace EventStore

open Azure.TableOperations
open EventStore.Core.Language

module Utilities =

    let valueFromRowKey       (RowKey       v) = v
    let valueFromPartitionKey (PartitionKey v) = v
    let valueFromJson         (JSON         v) = v
    let valueFromData         (Data         v) = valueFromJson v
    let valueFromMeta         (MetaData     v) = v
    let valueFromEventType    (EventType    v) = v
    let valueFromStreamId     (Stream       v) = v