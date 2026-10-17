module PipeL4BenchmarkGeneratedOutput

open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let length = int input.Bytes.Length
                let destination = output.GetSpan(length)
                let mutable offset = 0

                for segment in input.Bytes do
                    segment.Span.CopyTo(destination.Slice(offset))
                    offset <- offset + segment.Length

                output.Advance(length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
