module Azure.EventStore.Tests

open NUnit.Framework

open Azure.EventStore.TestAPI.Mock
open Azure

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

[<Test>]
let ``Integration: Append event`` () =

    async {

        // Setup
        match! someConnectionRequest |> EventStore.tryConnect with
        | Error msg     -> Assert.Fail msg
        | Ok connection -> 
             
            async {

                // Test
                match! EventStore.tryAppend someStream someEvent someConnectionString with
                | Error msg -> failwith msg
                | Ok _      -> teardown connection

            } |> Async.RunSynchronously

    } |> Async.RunSynchronously