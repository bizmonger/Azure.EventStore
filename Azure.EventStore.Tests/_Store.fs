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
        let partitionKey = someEvent.Stream |> valueFromStreamId |> PartitionKey

        match! someConnectionString |> EventStore.tryAppend someEvent with
        | Error msg -> failwith msg
        | Ok _      ->

            // Test
            match! someConnectionString |> TableOperations.tryReadBackwardsCount SomeStreamTable partitionKey 1 with
            | Error msg -> failwith msg
            | Ok events -> events |> Seq.isEmpty |> should equal false
    
    } |> Async.RunSynchronously

[<Test>]
let ``Read last 2 events from EventStore (descending)`` () =

    async {

        // Setup
        let partitionKey = someEvent.Stream |> valueFromStreamId |> PartitionKey

        do! someConnectionString |> EventStore.tryAppend someEvent  |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someEvent2 |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someEvent3 |> Async.Ignore

        // Test
        match! someConnectionString |> EventStore.tryReadBackwardsCount someEvent.Stream 2 with
        | Error msg -> failwith msg
        | Ok events ->
             events |> Seq.map(fun v -> v.Data) |> should equal <| seq [someEvent3.Data; someEvent2.Data]

    } |> Async.RunSynchronously