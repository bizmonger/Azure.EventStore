module _Store

open NUnit.Framework
open FsUnit
open Azure.EventStore.TestAPI.Mock
open Azure

let teardown() =

    () // TODO...

let ``Add event to EventStore`` () =

    async {

        // Test
        match! someConnectionString |> EventStore.tryAppend someStream someEvent with
        | Error msg -> failwith msg
        | Ok _      -> teardown()
    
    } |> Async.RunSynchronously

let ``Read event from EventStore`` () =

    async {

        // Setup
        match! someConnectionString |> EventStore.tryAppend someStream someEvent with
        | Error msg -> failwith msg
        | Ok _      -> ()
    
    } |> Async.RunSynchronously