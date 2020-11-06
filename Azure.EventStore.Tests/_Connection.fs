module Azure.EventStore.Tests

open NUnit.Framework

open Azure
open Azure.EventStore.TestAPI.Mock

let teardown connection =

    async {

        match! connection |> EventStore.tryTerminate with
        | Error msg -> failwith msg
        | Ok _      -> ()
            
    } |> Async.RunSynchronously

[<Test>]
let ``Integration: Create connection`` () =

    async {

        // Test
        match! someConnectionRequest |> EventStore.tryConnect with
        | Error msg -> failwith msg
        | Ok connection -> 
        
            // Teardown
            teardown connection

    } |> Async.RunSynchronously

[<Test>]
let ``Integration: Terminate connection`` () =

    async {

        // Setup
        match! someConnectionRequest |> EventStore.tryConnect with
        | Error msg     -> failwith msg
        | Ok connection -> 
        
            // Test
            match! connection |> EventStore.tryTerminate with
            | Error msg -> failwith msg
            | Ok _      ->

                // Teardown
                async {

                    match! connection |> EventStore.tryTerminate with
                    | Error msg -> failwith msg
                    | Ok _      -> ()
                        
                } |> Async.RunSynchronously

    } |> Async.RunSynchronously