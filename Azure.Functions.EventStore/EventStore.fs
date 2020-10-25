namespace Azure

module EventStore =

    open Microsoft.WindowsAzure.Storage
    open EventStore.Operations
    open EventStore.Language
    open Azure
    open Microsoft.WindowsAzure.Storage.Table

    let tryTerminate : Terminate =

        fun connection ->

            async {

                try
                    // Not clear if anything needs to be done with the following:
                    // connection.Context :?> CloudStorageAccount
                    return Ok ()

                with ex -> return Error <| ex.GetBaseException().Message
            
            }

    let tryAppend : AppendToStream =

        fun stream event -> 
        
            async { 

                return Error "not implemented"
            
                //let entity = TableEntity() 
                //Table.create 
                //return Error "not implemented" 
                
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
                            AppendToStreamAsync      = tryAppend
                            ReadStreamEventsBackward = fun _ _ _ -> async { return Error "not implemented" }
                            Terminate                = tryTerminate
                        }

                        return Ok result

                with ex -> return Error <| ex.GetBaseException().Message
            }

    let tryReadBackwards : ReadStreamEventsBackward =

        fun _ _ _ -> async { return Error "not implemented" }