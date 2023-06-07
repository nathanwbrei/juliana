"""
# module Engine

- Julia version: 
- Author: nbrei
- Date: 2023-06-05

# Examples

```jldoctest
julia>
```
"""
module Engine

    mutable struct Arrow
        name::String
        execute::Function
        data::Any
        input_channel::Channel
        output_channel::Channel
        max_parallelism::Int64
        worker_tasks::Vector{Task}
        shutdown_task::Union{Task,Nothing}
    end

    import Base.Threads.@spawn

    function run(arrows; nthreads=Threads.nthreads())
        println("Starting run() with nthreads = $(nthreads)")
        for arrow in arrows
            for i in 1:arrow.max_parallelism
                push!(arrow.worker_tasks, @spawn begin; println("Launched worker task $(i) for arrow $(arrow.name)"); arrow.execute(arrow.data, arrow.input_channel, arrow.output_channel); end)
            end

            arrow.shutdown_task = @spawn begin
                println("Launched shutdown task for arrow $(arrow.name)")
                for w in arrow.worker_tasks
                    wait(w)
                end
                close(arrow.output_channel)
            end
            println("Started arrow $(arrow.name)")
        end
        println("All workers have started")
        try
            Base.exit_on_sigint(false)
            for arrow in arrows
                wait(arrow.shutdown_task)
                println("Shut down arrow $(arrow.name)")
            end
            println("All worker threads joined")
        catch ex
            @show ex
            if isa(ex, TaskFailedException)
                println(ex.task.exception)
            end
            if isa(ex, InterruptException)
                println("Interrupted! Exiting")
            end
        end
    end

    function test_source(data, input_ch, output_ch)
        for i in 1:200
            try
                event = take!(input_ch)
                push!(event, "emit $(i)")
                println("emit event nr $(i) on thread $(Threads.threadid())")
                put!(output_ch, event)
            catch
                println("Source: Input channel closed! Worker shutting down.")
                break
            end
        end
    end

    function test_map(data, input_ch, output_ch)
        while true
            try
                event = take!(input_ch)
                push!(event, "map")
                println("map(): $(event) on thread $(Threads.threadid())")
                put!(output_ch, event)
            catch
                println("Map: Input channel closed! Worker shutting down.")
                break;
            end
        end
    end

    function test_reduce(data, input_ch, output_ch)
        while true
            try
                event = take!(input_ch)
                push!(event, "reduce")
                println("reduce(): $(event) on thread $(Threads.threadid())")
                empty!(event)
                put!(output_ch, event)
            catch
                println("Reduce: Input channel closed! Worker shutting down.")
                break;
            end
        end
    end

    function run_basic_example()
        println("Running basic example")
        pool = Channel(10)
        emitted = Channel(10)
        mapped = Channel(10)
        for i in 1:8
            put!(pool, [])
        end
        source = Arrow("source", test_source, nothing, pool, emitted, 1, [], nothing)
        map = Arrow("map", test_map, nothing, emitted, mapped, 4, [], nothing)
        reduce = Arrow("reduce", test_reduce, nothing, mapped, pool, 1, [], nothing)
        topology = [source, map, reduce]
        run(topology)

        for arrow in topology
            for (id,task) in enumerate(arrow.worker_tasks)
                if (istaskfailed(task))
                    println("Arrow $(arrow.name): worker $(id): $(task.result)")
                else
                    println("Arrow $(arrow.name): worker $(id): Success")
                end
            end
            if (istaskfailed(arrow.shutdown_task))
                println("Arrow $(arrow.name): shutdown: $(arrow.shutdown_task.result)")
            else
                println("Arrow $(arrow.name): shutdown: Success")
            end
        end
    end
end

Engine.run_basic_example()
