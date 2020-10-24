
module EventStore

    open Microsoft.WindowsAzure.Storage
    open EventStore.Operations
    open EventStore.Language

    let tryConnect : Create =

        fun request -> 
        
            async {
            
                try
                    //let storageAccount   = CloudStorageAccount.Parse request.ConnectionString
                    //let cloudTableClient = storageAccount.CreateCloudTableClient()

                    //if obj.ReferenceEquals(cloudTableClient, null) then
                    //    return Error "Connection failed" 

                    //else 

                        let result : Connection = { 
                            Context                  = obj() //cloudTableClient
                            ConnectionString         = request.ConnectionString
                            AppendToStreamAsync      = fun _ _   -> async { return Error "not implemented" }
                            ReadStreamEventsBackward = fun _ _ _ -> async { return Error "not implemented" }
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

    let tryAppend : AppendToStream =

        fun _ _ -> async { return Error "not implemented" }