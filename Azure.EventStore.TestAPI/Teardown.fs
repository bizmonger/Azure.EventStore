namespace Azure.EventStore.TestAPI

open Azure.Storage

module Teardown =

    let execute (storageConnectionString:string) entityId = 
    
        let connectionstring = ConnectionString storageConnectionString
        let table            = Table        <| "Event"
        let partitionKey     = PartitionKey <| "EventEntity"
        let rowKey           = RowKey       <| entityId.ToString()

        async {

            let! findResult = ensureExists connectionstring table |> Async.AwaitTask

            findResult |> function
            | Error msg'    -> failwith msg'
            | Ok cloudTable ->
            
                async {
            
                    match! cloudTable |> deleteEntity partitionKey rowKey |> Async.AwaitTask with
                    | Error e -> failwith e
                    | Ok _    -> ()

                } |> Async.RunSynchronously
        }