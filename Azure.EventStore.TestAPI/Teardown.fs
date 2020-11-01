namespace Azure.EventStore.TestAPI

open System.Net
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open EventStore.Core.Language
open EventStore.Language

module Teardown =

    let execute() = 
    
        ()