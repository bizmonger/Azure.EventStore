﻿namespace EventStore.Core

open System

module Language =

    type JSON   = JSON   of string
    type Stream = Stream of string
    type Data   = Data   of JSON

    type EventId   = string

    type Event = {
        Id        : EventId
        Data      : Data
        Timestamp : DateTime
    }