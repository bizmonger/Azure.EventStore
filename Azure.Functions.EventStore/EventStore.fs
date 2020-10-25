namespace Azure

open System
open EventStore.Core.Language

module EventStore =

    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table
    open EventStore.Operations
    open EventStore.Language
    open Azure

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
       
            match! tableName |> Table.create entity (Azure.Table.ConnectionString connectionString) with
            | Error msg -> return Error msg
            | Ok _      -> return Ok ()
        }

    let tryAppend : AppendToStream =

        fun (Stream stream) event connectionString -> 
        
            async {
            
                try

                    let streamEntity = StreamEntity(stream,  PartitionKey="Stream", RowKey=stream)
                    let eventEntity  = EventEntity(event.Id, PartitionKey="Event" , RowKey=Guid.NewGuid().ToString())

                    match! "Stream" |> Table.create streamEntity (Azure.Table.ConnectionString connectionString) with
                    | Error msg -> return Error msg
                    | Ok _      -> 

                        match! "Event" |> Table.create eventEntity (Azure.Table.ConnectionString connectionString) with
                        | Error msg -> return Error msg
                        | Ok _      -> return Ok ()

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

        fun _ _ _ _ -> async { return Error "not implemented" }