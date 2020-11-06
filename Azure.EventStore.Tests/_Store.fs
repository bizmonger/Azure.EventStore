﻿module _Store

open NUnit.Framework
open FsUnit
open Azure
open Azure.EventStore.TestAPI
open Azure.EventStore.TestAPI.Mock
open Azure.Entities
open Azure.TableOperations
open EventStore.Utilities
open EventStore.Core.Language

[<TearDown>]
let teardown() = 

        let (Stream stream) = someStream

        seq [SomeStreamTable, PartitionKey stream
            ] |> Seq.iter (fun v -> async { do! Teardown.execute v } |> Async.RunSynchronously)
    
[<Test>]
let ``Add event to EventStore`` () =

    async {

        // Test
        match! someConnectionString |> EventStore.tryAppend someEvent with
        | Error msg -> failwith msg
        | Ok _      -> ()
    
    } |> Async.RunSynchronously

[<Test>]
let ``Read event from EventStore`` () =

    async {

        // Setup
        match! someConnectionString |> EventStore.tryAppend someEvent with
        | Error msg -> failwith msg
        | Ok _      ->

            // Test
            match! (someEvent.Stream |> valueFromStreamId |> PartitionKey, 1) ||> TableOperations.tryReadBackwardsCount SomeStreamTable someConnectionString with
            | Error msg -> failwith msg
            | Ok events -> events |> Seq.isEmpty |> should equal false
    
    } |> Async.RunSynchronously

[<Test>]
let ``Read last 2 events from EventStore (descending)`` () =

    async {

        // Setup
        do! someConnectionString |> EventStore.tryAppend someEvent  |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someEvent2 |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someEvent3 |> Async.Ignore

        // Test
        match! (someEvent.Stream |> valueFromStreamId |> PartitionKey, 2) ||> TableOperations.tryReadBackwardsCount<EventEntity> SomeStreamTable someConnectionString with
        | Error msg -> failwith msg
        | Ok events ->
             events |> Seq.map(fun v -> v.Data) |> should equal <| seq [someEvent3.Data; someEvent2.Data]

    } |> Async.RunSynchronously