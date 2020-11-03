module _Store

open NUnit.Framework
open FsUnit
open Azure
open Azure.EventStore.TestAPI
open Azure.EventStore.TestAPI.Mock
open Azure.Storage

[<TearDown>]
let teardown() = 

        seq [Table "Event" , PartitionKey "Event"
             Table "Stream", PartitionKey "Stream"

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

        // Test
        match! someConnectionString |> EventStore.tryReadBackwards someStream startIndex count with
        | Error msg -> failwith msg
        | Ok events ->
             events |> Seq.isEmpty |> should equal false
    
    } |> Async.RunSynchronously