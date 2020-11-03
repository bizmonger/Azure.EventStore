namespace Azure

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open EventStore.Core.Language
open EventStore.Operations
open EventStore.Language
open Azure.Storage

module EventStore =

    let tryTerminate : Terminate =

        fun connection ->

            async {

                try
                    // Not clear if anything needs to be done with the following:
                    // connection.Context :?> CloudStorageAccount
                    return Ok ()

                with ex -> return Error <| ex.GetBaseException().Message
            
            }

    let private create (connectionString:string) (v:TableEntity*string) =

        let entity,tableName = v

        async {

            try
                match! tableName |> create entity (ConnectionString connectionString) with
                | Error msg -> return Error msg
                | Ok _      -> return Ok ()

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let tryAppend : AppendToStream =

        fun (Stream stream) event connectionString -> 
        
            async {

                let (Data data) = event.Data
                let (JSON json) = data
            
                try
                    let streamEntity = StreamEntity(stream,  PartitionKey="Stream", RowKey=stream)
                    let eventEntity  = EventEntity(event.Id, PartitionKey="Event" , 
                                                             RowKey= Guid.NewGuid().ToString(),
                                                             Stream= stream,
                                                             Data=   json)

                    let addEntry = create connectionString

                    let isError = function | Ok _ -> false | Error _ -> true

                    let! ops = [addEntry (streamEntity :> TableEntity, "Stream")
                                addEntry (eventEntity  :> TableEntity, "Event" )
                               ] |> Async.Parallel

                    return
                        ops |> Array.exists isError
                            |> function
                               | true  -> Error <| sprintf "Failed to append event to stream: %s %s" event.Id stream
                               | false -> Ok ()

                with ex -> return Error <| ex.GetBaseException().Message
            }

    let tryConnect : Create =

        fun request -> 
        
            async {
            
                try
                    let storageAccount   = CloudStorageAccount.Parse request.ConnectionString
                    let cloudTableClient = storageAccount.CreateCloudTableClient()

                    if obj.ReferenceEquals(cloudTableClient, null) then
                        return Error "Connection failed" 

                    else 
                        let result : Connection = { 
                            Context                  = cloudTableClient
                            ConnectionString         = request.ConnectionString
                        }

                        return Ok result

                with ex -> return Error <| ex.GetBaseException().Message
            }

    let tryReadBackwards : ReadStreamEventsBackward =

        fun (Stream stream) startIndex count connectionString -> 

            let toEvent (v:EventEntity) = { 
                Event.Id  = v.RowKey; 
                Data      = Data (JSON v.Data); 
                Timestamp = v.Timestamp.DateTime
            }

            async { 

                match! (Table stream) |> ensureExists (ConnectionString connectionString) |> Async.AwaitTask with
                | Error msg     -> return Error msg
                | Ok cloudTable ->
                     
                     match! cloudTable |> getEntitiesAsync |> Async.AwaitTask with
                     | Error msg'  -> return Error msg'
                     | Ok entities ->

                        let events = 
                            entities |> Seq.cast<EventEntity>
                                     |> Seq.map toEvent
                                     |> Seq.sortByDescending(fun v -> v.Timestamp)
                                     |> Seq.skip startIndex
                                     |> Seq.take count
                     
                        return Ok events
            }