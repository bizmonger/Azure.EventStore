module _Store

open NUnit.Framework
open Azure
open Azure.EventStore.TestAPI
open Azure.EventStore.TestAPI.Mock
open Azure.Storage

[<SetUp>]
let setup() = 

    seq [Table "Event" , PartitionKey "Event"
         Table "Stream", PartitionKey "Stream"

        ] |> Seq.iter (fun v -> async { do! Teardown.execute v } |> Async.RunSynchronously)

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
        match! someConnectionString |> EventStore.tryAppend someStream someEvent with
        | Error msg -> failwith msg
        | Ok _      -> ()
    
    } |> Async.RunSynchronously