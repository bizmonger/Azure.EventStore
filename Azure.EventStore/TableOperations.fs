namespace Azure

open System
open System.Linq
open System.Net
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open EventStore.Language
open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Azure.EventStore.TestAPI")>]

do()

module Entities =

    type EventEntity() =

        inherit TableEntity()

        member val Stream    = "" with get,set
        member val EventType = "" with get,set
        member val MetaData  = "" with get,set
        member val Data      = "" with get,set

module TableOperations =

    type Table            = Table            of string
    type PartitionKey     = PartitionKey     of string
    type RowKey           = RowKey           of string

    let tryEnsureExists connectionString (Table tableName) =

        async {

            try
                let storageAccount   = CloudStorageAccount.Parse connectionString
                let cloudTableClient = storageAccount.CreateCloudTableClient()
                let cloudTable       = cloudTableClient.GetTableReference(tableName);

                let! exists = cloudTable.ExistsAsync() |> Async.AwaitTask

                if not exists then
        
                    let! success = cloudTable.CreateIfNotExistsAsync() |> Async.AwaitTask

                    if   success
                    then return Ok cloudTable
                    else return Error <| sprintf "Failed to create: %s" tableName

                else return Ok cloudTable
                    

            with
                ex -> return Error <| sprintf "%s\n\n%s" (ex.GetBaseException().Message) ex.StackTrace

        } |> Async.StartAsTask
    
    let private tryFind<'T when 'T :> TableEntity> (PartitionKey partitionKey) (RowKey rowKey) (storageTable:CloudTable) =

        async {
            
            try
                let  operation      = TableOperation.Retrieve<'T>(partitionKey, rowKey)
                let! retrieveResult = storageTable.ExecuteAsync(operation) |> Async.AwaitTask

                if retrieveResult.Result <> null then
                     return Ok <| Some (retrieveResult.Result :?> 'T)
                else return Ok <| None

            with ex -> return Error <| ex.GetBaseException().Message

        } |> Async.StartAsTask

    let private tryDelete<'T when 'T :> TableEntity> (PartitionKey partitionKey) (RowKey rowKey) (cloudTable:CloudTable) =

        async {
            
            let  operation      = TableOperation.Retrieve<'T>(partitionKey, rowKey)
            let! retrieveResult = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

            if retrieveResult.Result <> null then

                 let  entity    = retrieveResult.Result :?> 'T
                 let  operation = TableOperation.Delete entity
                 let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

                 if result.HttpStatusCode = (int)HttpStatusCode.NoContent
                 then return Ok ()
                 else return Error <| sprintf "%i: Failed to delete entity" result.HttpStatusCode
            
            else 
                 return Error <| sprintf "Failed to delete %s::%s" partitionKey rowKey

        } |> Async.StartAsTask

    let internal tryDeleteAll<'T when 'T :> TableEntity> (PartitionKey partitionKey) (cloudTable:CloudTable) =

        async {
            
            try
                let batchOperation = TableBatchOperation()
                let filter         = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
                                
                let  rowKeyItem = System.Collections.Generic.List<string>(seq ["RowKey"])
                let  query      = TableQuery<DynamicTableEntity>().Where(filter).Select(rowKeyItem)
                let! result     = cloudTable.ExecuteQuerySegmentedAsync<_>(query, null) |> Async.AwaitTask

                for item in result  do
                    batchOperation.Delete item
            
                do! cloudTable.ExecuteBatchAsync(batchOperation) |> Async.AwaitTask |> Async.Ignore

                return Ok ()

            with ex -> return Error <| ex.GetBaseException().Message

        } |> Async.StartAsTask

    let private tryReadForward'<'T when 'T : (new : unit -> 'T :> TableEntity)> (partitionKey:string) (cloudTable:CloudTable) =

        async {
        
            let filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
            let query  = TableQuery<'T>().Where(filter)

            return! cloudTable.ExecuteQuerySegmentedAsync(query,null) |> Async.AwaitTask

        } |> Async.StartAsTask

    let private tryReadForwardCount'<'T when 'T : (new : unit -> 'T :> TableEntity)> (partitionKey:string) (count:int) (cloudTable:CloudTable) =

        async {
        
            try
                let filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
                let query  = TableQuery<'T>().Where(filter).Take((Nullable<int>)count)

                let! result = cloudTable.ExecuteQuerySegmentedAsync(query,null) |> Async.AwaitTask
                return Ok result

            with ex -> return Error <| ex.GetBaseException().Message

        } |> Async.StartAsTask

    let private tryReadBackwards'<'T when 'T : (new : unit -> 'T :> TableEntity)> (partitionKey:string) (cloudTable:CloudTable) =

        async {
    
            let filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
            let query  = TableQuery<'T>().Where(filter)

            return! cloudTable.ExecuteQuerySegmentedAsync(query,null) |> Async.AwaitTask

        } |> Async.StartAsTask

    let private tryUpdate (cloudTable:CloudTable) (entity:TableEntity) = 

        async {

            try
                let  pKey,rKey = PartitionKey entity.PartitionKey, RowKey entity.RowKey
                let! result    = cloudTable |> tryFind pKey rKey |> Async.AwaitTask

                return
                    match result with
                    | Error msg -> Error msg
                    | Ok v      ->
                         v |> function
                              | None -> Error <| sprintf "Entity not found: %A" entity
                              | Some _ ->
                              
                                async {
                                
                                    let  operation = TableOperation.Replace(entity)
                                    let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask
          
                                    if result.HttpStatusCode = (int)HttpStatusCode.NoContent
                                    then return Ok    <| (result.Result :?> TableEntity)
                                    else return Error <| sprintf "%i: Failed to update entity" result.HttpStatusCode

                                } |> Async.RunSynchronously

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let internal tryCreate (entity:TableEntity) (connectionString:string) (tableName:string) =

        async {
        
            try
                let table = Table tableName

                match! table |> tryEnsureExists connectionString |> Async.AwaitTask with
                | Error msg     -> return Error msg
                | Ok cloudTable ->

                    let  operation = TableOperation.Insert(entity)
                    let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask
    
                    if result.HttpStatusCode = (int)HttpStatusCode.NoContent
                    then return Ok ()
                    else return Error <| result.HttpStatusCode.ToString()

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let private tryGetCloudTable (tableName:string) (connectionString:string) =

        try
            let storageAccount   = CloudStorageAccount.Parse connectionString
            let cloudTableClient = storageAccount.CreateCloudTableClient()
            let cloudTable       = cloudTableClient.GetTableReference(tableName)
            Ok  cloudTable

        with ex -> Result.Error <| ex.GetBaseException().Message

    let internal tryReadForward<'T when 'T : (new : unit -> 'T :> TableEntity)> (PartitionKey partitionKey) (table:Table) (connectionstring:ConnectionString) =

        async {

            try
                let! result = tryEnsureExists connectionstring table |> Async.AwaitTask

                return
                    result |> function
                    | Error msg     -> Result.Error msg
                    | Ok cloudTable ->

                        async {

                            let! entities = cloudTable |> tryReadForward'<'T> partitionKey |> Async.AwaitTask

                            return Result.Ok <| entities.Results.OrderBy(fun e -> e.Timestamp)

                        } |> Async.RunSynchronously

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let internal tryReadForwardCount<'T when 'T : (new : unit -> 'T :> TableEntity)> (table:Table) (PartitionKey partitionKey) (count:int) (connectionstring:ConnectionString) =

        async {

            try
                let! result = tryEnsureExists connectionstring table |> Async.AwaitTask

                return
                    result |> function
                    | Error msg     -> Result.Error msg
                    | Ok cloudTable ->

                        async {

                            match! cloudTable |> tryReadForwardCount'<'T> partitionKey count |> Async.AwaitTask with
                            | Error msg   -> return Error msg
                            | Ok entities -> return Ok <| entities.Results.OrderBy(fun e -> e.Timestamp)

                        } |> Async.RunSynchronously

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let internal tryReadBackwards<'T when 'T : (new : unit -> 'T :> TableEntity)> (PartitionKey partitionKey) (table:Table) (connectionstring:ConnectionString) =

        async {

            try
                let! result = tryEnsureExists connectionstring table |> Async.AwaitTask

                return
                    result |> function
                    | Error msg     -> Result.Error msg
                    | Ok cloudTable ->

                        async {

                            let! entities = cloudTable |> tryReadForward'<'T> partitionKey |> Async.AwaitTask
                            let  result   = entities.Results.OrderByDescending(fun e -> e.Timestamp)

                            return Result.Ok result

                        } |> Async.RunSynchronously

            with ex -> return Error <| ex.GetBaseException().Message
        }

    let internal tryReadBackwardsCount<'T when 'T : (new : unit -> 'T :> TableEntity)> (table:Table) (PartitionKey partionKey) (count:int) (connectionstring:ConnectionString)  =

        async {

            try
                let! result = tryEnsureExists connectionstring table |> Async.AwaitTask

                return
                    result |> function
                    | Error msg     -> Result.Error msg
                    | Ok cloudTable ->

                        async {

                            let! entities = cloudTable |> tryReadBackwards'<'T> partionKey |> Async.AwaitTask

                            return Result.Ok <| entities.Results.OrderByDescending(fun e -> e.Timestamp)
                                                                .Take count

                        } |> Async.RunSynchronously

            with ex -> return Error <| ex.GetBaseException().Message
        }