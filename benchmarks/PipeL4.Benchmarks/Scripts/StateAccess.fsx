module StateAccess

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let key = "benchmark-shared-state"
    let factory = fun () -> obj()
    let initialized = connectionState.GetOrAdd(key, factory)

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let current = connectionState.GetOrAdd(key, factory)

                if not (Object.ReferenceEquals(initialized, current)) then
                    invalidOp "The connection-state lookup returned a different value."

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
