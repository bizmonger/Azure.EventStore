namespace Azure.EventStore

open System.Net
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open EventStore.Operations

module AzureTable =

    type ConnectionString = ConnectionString of string
    type Table            = Table            of string
    type PartitionKey     = PartitionKey     of string
    type RowKey           = RowKey           of string

    let ensureExists (ConnectionString connectionString) (Table tableName) =

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
    
    let findEntity<'T when 'T :> TableEntity> (PartitionKey partitionKey) (RowKey rowKey) (storageTable:CloudTable) =

        async {
            
            let  operation      = TableOperation.Retrieve<'T>(partitionKey, rowKey)
            let! retrieveResult = storageTable.ExecuteAsync(operation) |> Async.AwaitTask

            if retrieveResult.Result <> null then
                 return Some (retrieveResult.Result :?> 'T)
            else return None

        } |> Async.StartAsTask

    let updateEntity (cloudTable:CloudTable) (entity:TableEntity) = 

        async {

            let pKey,rKey = PartitionKey entity.PartitionKey, RowKey entity.RowKey

            match! cloudTable |> findEntity pKey rKey |> Async.AwaitTask with
            | None -> return Error <| sprintf "Entity not found: %A" entity
            | Some _ ->

                let  operation = TableOperation.Replace(entity)
                let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask

                if result.HttpStatusCode = (int)HttpStatusCode.NoContent
                then return Ok    <| (result.Result :?> TableEntity)
                else return Error <| sprintf "%i: Failed to update entity" result.HttpStatusCode
        }

    let internal create (entity:TableEntity) (connectionstring:ConnectionString) (tableName:string) =

        async {
        
            let table = Table tableName

            match! table |> ensureExists connectionstring |> Async.AwaitTask with
            | Error msg     -> return Error msg
            | Ok cloudTable ->

                let  operation = TableOperation.Insert(entity)
                let! result    = cloudTable.ExecuteAsync(operation) |> Async.AwaitTask
                
                if result.HttpStatusCode = (int)HttpStatusCode.NoContent
                then return Ok ()
                else return Error <| result.HttpStatusCode.ToString()
        }