namespace EventStore.Core

open System

module Language =

    type JSON      = JSON      of string
    type Stream    = Stream    of string
    type Data      = Data      of JSON
    type MetaData  = MetaData  of string
    type EventType = EventType of string

    type EventId   = string

    type Event = {
        Id        : EventId
        Stream    : Stream
        Data      : Data
        MetaData  : MetaData
        EventType : EventType
        Timestamp : DateTime
    }