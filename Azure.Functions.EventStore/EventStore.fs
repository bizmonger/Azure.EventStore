namespace Azure

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open EventStore.Core.Language
open EventStore.Operations
open EventStore.Language

//module EventStore =

//    let tryTerminate : Terminate =

//        fun connection ->

//            async {

//                try
//                    // Not clear if anything needs to be done with the following:
//                    // connection.Context :?> CloudStorageAccount
//                    return Ok ()

//                with ex -> return Error <| ex.GetBaseException().Message
            
//            }

//    let private appendTo stream (entity:TableEntity) (connectionString:string) =

//        async {

//            try
//                match! stream |> create entity (ConnectionString connectionString) with
//                | Error msg -> return Error msg
//                | Ok _      -> return Ok ()

//            with ex -> return Error <| ex.GetBaseException().Message
//        }

//    let tryAppend : AppendToStream =

//        fun (Stream stream) event connection -> 
        
//            async {

//                let (Data      data)      = event.Data
//                let (JSON      json)      = data
//                let (EventType eventType) = event.EventType
            
//                try
//                    let eventEntity  = EventEntity(PartitionKey="Event" , 
//                                                   RowKey    = Guid.NewGuid().ToString(),
//                                                   Stream    = stream,
//                                                   EventType = eventType,
//                                                   Data      = json)

//                    return! connection |> appendTo stream eventEntity

//                with ex -> return Error <| ex.GetBaseException().Message
//            }

//    let tryConnect : Create =

//        fun request -> 
        
//            async {
            
//                try
//                    let storageAccount   = CloudStorageAccount.Parse request.ConnectionString
//                    let cloudTableClient = storageAccount.CreateCloudTableClient()

//                    if obj.ReferenceEquals(cloudTableClient, null) then
//                        return Error "Connection failed" 

//                    else 
//                        let result : Connection = { 
//                            Context          = cloudTableClient
//                            ConnectionString = request.ConnectionString
//                        }

//                        return Ok result

//                with ex -> return Error <| ex.GetBaseException().Message
//            }

//    let tryReadBackwards : ReadStreamEventsBackward =

//        fun (Stream stream) startIndex count connectionString -> 

//            let toEvent (v:EventEntity) = { 
//                Event.Id  = v.RowKey;
//                EventType = EventType  v.EventType
//                Data      = Data (JSON v.Data); 
//                Timestamp = v.Timestamp.DateTime
//            }

//            async { 

//                match! (Table stream) |> exists (ConnectionString connectionString) |> Async.AwaitTask with
//                | Error msg -> return Error msg
//                | Ok result ->

//                    return
//                         result |> function
//                         | None            -> Ok <| seq []
//                         | Some cloudTable ->
                     
//                            async {
                            
//                                match! cloudTable |> getEntities<EventEntity> |> Async.AwaitTask with
//                                | Error msg'  -> return Error msg'
//                                | Ok entities ->

//                                   let events = 
//                                       entities |> Seq.map toEvent
//                                                |> Seq.sortByDescending(fun v -> v.Timestamp)
//                                                |> Seq.skip startIndex
//                                                |> Seq.take count
                     
//                                   return Ok events

//                            } |> Async.RunSynchronously
//            }