namespace Azure.EventStore.TestAPI

open Azure.Storage
open Mock

module Teardown =

    let execute((table,partitionKey):Table * PartitionKey) =

        async {
    
            let connectionString = ConnectionString someConnectionString

            match! table |> ensureExists connectionString |> Async.AwaitTask with
            | Error msg -> failwith msg
            | Ok cloudTable -> 
        
                match! cloudTable |> deleteEntities partitionKey |> Async.AwaitTask with
                | Error msg' -> failwith msg'
                | Ok _       -> ()
        }