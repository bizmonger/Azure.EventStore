namespace Azure.EventStore.TestAPI

open Mock
open Azure.TableOperations

module Teardown =

    let execute((table,partitionKey):Table * PartitionKey) =

        async {
    
            match! table |> tryEnsureExists someConnectionString |> Async.AwaitTask with
            | Error msg     -> failwith msg
            | Ok cloudTable -> 
        
                match! cloudTable |> tryDeleteAll partitionKey |> Async.AwaitTask with
                | Error msg' -> failwith msg'
                | Ok _       -> ()
        }