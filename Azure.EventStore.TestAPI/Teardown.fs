namespace Azure.EventStore.TestAPI

open Mock
open Azure.TableOperations

module Teardown =

    let execute((table,partitionKey):Table * PartitionKey) =

        async {
    
            match! table |> ensureExistsAsync someConnectionString |> Async.AwaitTask with
            | Error msg     -> failwith msg
            | Ok cloudTable -> 
        
                match! cloudTable |> deleteEntitiesAsync partitionKey |> Async.AwaitTask with
                | Error msg' -> failwith msg'
                | Ok _       -> ()
        }