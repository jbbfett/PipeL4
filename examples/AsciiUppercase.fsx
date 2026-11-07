open System.Threading.Tasks
open PipeL4.Plugins

let private toUpperAscii (value: byte) =
    if value >= byte 'a' && value <= byte 'z' then
        byte (int value - int 'a' + int 'A')
    else
        value

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()

            for segment in input.Bytes do
                let source = segment.Span

                if source.Length > 0 then
                    let destination = output.GetSpan(source.Length)

                    for index in 0 .. source.Length - 1 do
                        destination[index] <- toUpperAscii source[index]

                    output.Advance(source.Length)

            // This processor is stateless, so every byte is resolved immediately,
            // including the final invocation where input.IsCompleted is true.
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
