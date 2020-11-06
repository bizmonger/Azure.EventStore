namespace Azure

open System
open System.Linq
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Azure.Entities
open Azure.TableOperations
open EventStore.Core.Language
open EventStore.Operations
open EventStore.Language

module EventStore =

    let StreamTable = "Stream"

    let valueFromRowKey       (RowKey       v) = v
    let valueFromPartitionKey (PartitionKey v) = v
    let valueFromJson         (JSON         v) = v
    let valueFromData         (Data         v) = valueFromJson v
    let valueFromMeta         (MetaData     v) = v
    let valueFromEventType    (EventType    v) = v
    let valueFromStreamId     (Stream       v) = v

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
                            Context          = cloudTableClient
                            ConnectionString = request.ConnectionString
                        }
    
                        return Ok result
    
                with ex -> return Error <| ex.GetBaseException().Message
            }

    let tryTerminate : Terminate =
    
        fun connection ->
    
            async {
    
                try
                    // Not clear if anything needs to be done with the following:
                    // connection.Context :?> CloudStorageAccount
                    return Ok ()
    
                with ex -> return Error <| ex.GetBaseException().Message
                
            }

    let getCloudTable (tableName:string) (connectionString:string) =

        try
            let storageAccount   = CloudStorageAccount.Parse connectionString
            let cloudTableClient = storageAccount.CreateCloudTableClient()
            let cloudTable       = cloudTableClient.GetTableReference(tableName)
            Ok  cloudTable

        with ex -> Result.Error <| ex.GetBaseException().Message

    let tryAppend (event:Event) (connectionstring:ConnectionString) =

        async {
    
            try
                let partitionKey = PartitionKey <| (valueFromStreamId event.Stream)
                let rowKey       = RowKey       <| Guid.NewGuid().ToString()

                let entity = EventEntity()
                entity.RowKey       <- rowKey          |> valueFromRowKey
                entity.PartitionKey <- partitionKey    |> valueFromPartitionKey
                entity.Stream       <- event.Stream    |> valueFromStreamId
                entity.Data         <- event.Data      |> valueFromData
                entity.MetaData     <- event.MetaData  |> valueFromMeta
                entity.EventType    <- event.EventType |> valueFromEventType

                match! (connectionstring, StreamTable) ||> createAsync entity with
                | Error msg -> return Result.Error <| msg
                | Ok _      -> return Result.Ok    <| valueFromRowKey rowKey 

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let appendSequenceAsync (events:Event seq) (connectionstring:ConnectionString) (tableName:string) =

        async {

            let isSuccessful =  function
                | Error _ -> false
                | Ok    _ -> true
    
            return
                events 
                |> Seq.map (fun event -> async { return! tryAppend event connectionstring } |> Async.RunSynchronously)
                |> Seq.forall isSuccessful
                |> function
                    | false -> Result.Error "Failed to create an event"
                    | true  -> Result.Ok ()
        }

    let readEventsAscendingAsync (stream:Stream) (table:Table) (connectionstring:ConnectionString) =

        async {
    
            let! result = ensureExistsAsync connectionstring table |> Async.AwaitTask

            return
                result |> function
                | Error msg     -> Result.Error msg
                | Ok cloudTable ->

                    async {

                        let  streamId' = valueFromStreamId stream
                        let! entities  = cloudTable |> getEntitiesAsync streamId' |> Async.AwaitTask

                        return Result.Ok <| entities.Results.OrderBy(fun e -> e.Timestamp)

                    } |> Async.RunSynchronously
        }

    let readEventsForwardAsync<'T when 'T : (new : unit -> 'T :> TableEntity)> (streamId:Stream) (table:Table) (connectionstring:ConnectionString) =

        async {

            let! result = ensureExistsAsync connectionstring table |> Async.AwaitTask

            return
                result |> function
                | Error msg     -> Result.Error msg
                | Ok cloudTable ->

                    async {

                        let  streamId'= valueFromStreamId streamId
                        let! entities = cloudTable |> getEntitiesAsync streamId' |> Async.AwaitTask

                        return Result.Ok <| entities.Results.OrderBy(fun e -> e.Timestamp)

                    } |> Async.RunSynchronously
        }

    let readEventsForwardOnCountAsync<'T when 'T : (new : unit -> 'T :> TableEntity)> (table:Table) (connectionstring:ConnectionString) (stream:Stream) (count:int) =

        async {

            let! result = ensureExistsAsync connectionstring table |> Async.AwaitTask

            return
                result |> function
                | Error msg     -> Result.Error msg
                | Ok cloudTable ->

                    async {

                        let  streamId' = valueFromStreamId stream
                        let! entities  = cloudTable |> getEntitiessOnCountAsync streamId' count |> Async.AwaitTask

                        return Result.Ok <| entities.Results.OrderBy(fun e -> e.Timestamp)

                    } |> Async.RunSynchronously
        }

    let readEventsBackwardsAsync<'T when 'T : (new : unit -> 'T :> TableEntity)> (stream:Stream) (table:Table) (connectionstring:ConnectionString) =

        async {
    
            let! result = ensureExistsAsync connectionstring table |> Async.AwaitTask

            return
                result |> function
                | Error msg     -> Result.Error msg
                | Ok cloudTable ->

                    async {

                        let  streamId' = valueFromStreamId stream
                        let! entities  = cloudTable |> getEntitiesAsync<'T> streamId' |> Async.AwaitTask

                        let result = entities.Results.OrderByDescending(fun e -> e.Timestamp)

                        return Result.Ok result

                    } |> Async.RunSynchronously
        }

    let readEventsBackwardsOnCountAsync<'T when 'T : (new : unit -> 'T :> TableEntity)> (table:Table) (connectionstring:ConnectionString) (stream:Stream) (count:int) =

        async {
    
            let! result = ensureExistsAsync connectionstring table |> Async.AwaitTask

            return
                result |> function
                | Error msg     -> Result.Error msg
                | Ok cloudTable ->

                    async {

                        let  streamId' = valueFromStreamId stream
                        let! entities  = cloudTable |> getEntitiesBackwardsAsync<'T> streamId' |> Async.AwaitTask

                        return Result.Ok <| entities.Results.OrderByDescending(fun e -> e.Timestamp)
                                                            .Take count

                    } |> Async.RunSynchronously
        }

    //type ConnectionString = ConnectionString of string
    //type Table            = Table            of string
    //type PartitionKey     = PartitionKey     of string
    //type RowKey           = RowKey           of string

    //let exists (ConnectionString connectionString) (Table tableName) =

    //    async {
        
    //        try
    //            let storageAccount   = CloudStorageAccount.Parse connectionString
    //            let cloudTableClient = storageAccount.CreateCloudTableClient()
    //            let cloudTable       = cloudTableClient.GetTableReference(tableName);

    //            let! exists = cloudTable.ExistsAsync() |> Async.AwaitTask

    //            if exists 
    //            then return Ok <| Some cloudTable
    //            else return Ok <| None

    //        with ex -> return Error <| ex.GetBaseException().Message

    //    } |> Async.StartAsTask

    //let ensureExists (ConnectionString connectionString) (Table tableName) =

    //    async {

    //        try
    //            let storageAccount   = CloudStorageAccount.Parse connectionString
    //            let cloudTableClient = storageAccount.CreateCloudTableClient()
    //            let cloudTable       = cloudTableClient.GetTableReference(tableName);

    //            let! exists = cloudTable.ExistsAsync() |> Async.AwaitTask

    //            if not exists then
        
    //                let! success = cloudTable.CreateIfNotExistsAsync() |> Async.AwaitTask

    //                if   success
    //                then return Ok cloudTable
    //                else return Error <| sprintf "Failed to create: %s" tableName

    //            else return Ok cloudTable
                    

    //        with
    //            ex -> return Error <| sprintf "%s\n\n%s" (ex.GetBaseException().Message) ex.StackTrace

    //    } |> Async.StartAsTask
    
    //let findEntity<'T when 'T :> TableEntity> (PartitionKey partitionKey) (RowKey rowKey) (storageTable:CloudTable) =

    //    async {
            
    //        let  operation      = TableOperation.Retrieve<'T>(partitionKey, rowKey)
    //        let! retrieveResult = storageTable.ExecuteAsync(operation) |> Async.AwaitTask

    //        if retrieveResult.Result <> null then
    //             return Some (retrieveResult.Result :?> 'T)
    //        else return None

    //    } |> Async.StartAsTask

    //let getEntities<'T when 'T : (new : unit -> 'T :> TableEntity)> (storageTable:CloudTable) =

    //    async {
        
    //        try
    //            let    query = TableQuery<'T>()
    //            let!   task  = storageTable.ExecuteQuerySegmentedAsync(query,null) |> Async.AwaitTask

    //            return Ok task.Results

    //        with ex -> return Error <| ex.GetBaseException().Message

    //    } |> Async.StartAsTask

    //let updateEntity (cloudTable:CloudTable) (entity:TableEntity) = 

    //    async {

    //        let pKey,rKey = PartitionKey entity.PartitionKey, RowKey entity.RowKey

    //        match! cloudTable |> findEntity pKey rKey |> Async.AwaitTask with
    //        | None   -> return Error <| sprintf "Entity not found: %A" entity
    //        | Some _ ->

    //            let  operation = TableOperation.Replace(entity)
    //            let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

    //            if result.HttpStatusCode = (int)HttpStatusCode.NoContent
    //            then return Ok    <| (result.Result :?> TableEntity)
    //            else return Error <| sprintf "%i: Failed to update entity" result.HttpStatusCode
    //    }

    //let create (entity:TableEntity) (connectionstring:ConnectionString) (tableName:string) =

    //    async {
        
    //        let table = Table tableName

    //        match! table |> ensureExists connectionstring |> Async.AwaitTask with
    //        | Error msg     -> return Error msg
    //        | Ok cloudTable ->

    //            let  operation = TableOperation.Insert(entity)
    //            let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask
                
    //            if result.HttpStatusCode = (int)HttpStatusCode.NoContent
    //            then return Ok ()
    //            else return Error <| result.HttpStatusCode.ToString()
    //    }

    //let deleteEntity<'T when 'T :> TableEntity> (PartitionKey partitionKey) (RowKey rowKey) (cloudTable:CloudTable) =

    //    async {
        
    //        let  operation      = TableOperation.Retrieve<'T>(partitionKey, rowKey)
    //        let! retrieveResult = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

    //        if retrieveResult.Result <> null then

    //             let  entity    = retrieveResult.Result :?> 'T
    //             let  operation = TableOperation.Delete entity
    //             let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

    //             if result.HttpStatusCode = (int)HttpStatusCode.NoContent
    //             then return Ok ()
    //             else return Error <| sprintf "%i: Failed to delete entity" result.HttpStatusCode
        
    //        else 
    //             return Error <| sprintf "Failed to delete %s::%s" partitionKey rowKey

    //    } |> Async.StartAsTask

    //let deleteEntities<'T when 'T :> TableEntity> (PartitionKey partitionKey) (cloudTable:CloudTable) =

    //    async {
            
    //        try
    //            let batchOperation = TableBatchOperation()
    //            let filter         = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
                            
    //            let  rowKeyItem = System.Collections.Generic.List<string>(seq ["RowKey"])
    //            let  query      = TableQuery<DynamicTableEntity>().Where(filter).Select(rowKeyItem)
    //            let! result     = cloudTable.ExecuteQuerySegmentedAsync<_>(query, null) |> Async.AwaitTask

    //            for item in result  do
    //                batchOperation.Delete item
        
    //            if batchOperation.Count = 0
    //            then return Ok ()
    //            else 
    //                do! cloudTable.ExecuteBatchAsync(batchOperation) |> Async.AwaitTask |> Async.Ignore
    //                return Ok ()

    //        with ex -> return Error <| ex.GetBaseException().Message

    //    } |> Async.StartAsTask