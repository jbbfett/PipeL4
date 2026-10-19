module CachedState

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let cached = connectionState.GetOrAdd("benchmark-shared-state", fun () -> obj())

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty && Object.ReferenceEquals(cached, null) then
                invalidOp "The cached connection-state value was null."

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
