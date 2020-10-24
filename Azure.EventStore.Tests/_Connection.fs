module Azure.EventStore.Tests

open NUnit.Framework

open Azure.EventStore.TestAPI.Mock

[<Test>]
let ``Integration: Create connection`` () =

    async {

        // Test
        match! someConnectionRequest |> EventStore.tryConnect with
        | Error msg -> failwith msg
        | Ok _ -> 
        
        // Teardown
        ()

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
            | Ok _      -> ()

    } |> Async.RunSynchronously

[<Test>]
let ``Integration: Append event`` () =

    async {

        // Setup
        match! someConnectionRequest |> EventStore.tryConnect with
        | Error msg     -> Assert.Fail msg
        | Ok connection -> 
        
            async {

                match! someStream |> EventStore.tryAppend someEvent with
                | Error msg -> Assert.Fail msg
                | Ok _      -> ()
            
            } |> Async.RunSynchronously

            // Teardown
            match! connection |> EventStore.tryTerminate with
            | Error msg -> failwith msg
            | Ok _      -> ()

    } |> Async.RunSynchronously