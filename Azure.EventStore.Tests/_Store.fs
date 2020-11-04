module _Store

open NUnit.Framework
open FsUnit
open Azure
open Azure.EventStore.TestAPI
open Azure.EventStore.TestAPI.Mock
open Azure.Storage
open EventStore.Core.Language

[<TearDown>]
let teardown() = 

        let (Stream stream) = someStream

        seq [Table stream, PartitionKey "Stream"
            ] |> Seq.iter (fun v -> async { do! Teardown.execute v } |> Async.RunSynchronously)
    
[<Test>]
let ``Add event to EventStore`` () =

    async {

        // Test
        match! someConnectionString |> EventStore.tryAppend someStream someEvent with
        | Error msg -> failwith msg
        | Ok _      -> ()
    
    } |> Async.RunSynchronously

[<Test>]
let ``Read event from EventStore`` () =

    async {

        // Setup
        let startIndex, count = 0 , 1

        match! someConnectionString |> EventStore.tryAppend someStream someEvent with
        | Error msg -> failwith msg
        | Ok _      ->

            // Test
            match! someConnectionString |> EventStore.tryReadBackwards someStream startIndex count with
            | Error msg -> failwith msg
            | Ok events -> events |> Seq.isEmpty |> should equal false
    
    } |> Async.RunSynchronously

[<Test>]
let ``Read last 2 events from EventStore (descending)`` () =

    async {

        // Setup
        let startIndex, count = 0 , 2

        do! someConnectionString |> EventStore.tryAppend someStream someEvent  |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someStream someEvent2 |> Async.Ignore
        do! someConnectionString |> EventStore.tryAppend someStream someEvent3 |> Async.Ignore

        // Test
        match! someConnectionString |> EventStore.tryReadBackwards someStream startIndex count with
        | Error msg -> failwith msg
        | Ok events -> 
        
            let expected = events |> Seq.map(fun v -> v.Data) 

            expected |> should equal <| seq [someEvent3.Data; someEvent2.Data]

    } |> Async.RunSynchronously