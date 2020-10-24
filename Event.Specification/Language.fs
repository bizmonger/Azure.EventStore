namespace EventStore.Core

module Language =

    type JSON      = JSON      of string
    type Port      = Port      of int
    type Stream    = Stream    of string
    type Data      = Data      of JSON
    type MetaData  = MetaData  of string
    type EventType = EventType of string

    type EventId   = string

    type Event = {
        Id        : EventId
        Data      : Data
        MetaData  : MetaData
        EventType : EventType
    }