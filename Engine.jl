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
        state::Any
        input_channel::Channel
        output_channel::Channel
        max_parallelism::Int64
        worker_tasks::Vector{Task}
        shutdown_task::Union{Task,Nothing}
    end

    import Base.Threads.@spawn

    function worker(arrow::Arrow, id::Int64)
        println("Arrow '$(arrow.name)': Launched worker $(id)");
        while true
            try
                if arrow.input_channel == nothing
                    event = ()
                else
                    event = take!(arrow.input_channel)
                end
                result = arrow.execute(event, arrow.state)
                if (result == :shutdown)
                    println("Arrow '$(arrow.name)': worker $(id) shutting down.")
                    break;
                end
                println("Arrow '$(arrow.name)': worker $(id), thread $(Threads.threadid()): $(result)")
                if arrow.output_channel != nothing
                    put!(arrow.output_channel, result)
                end
            catch ex
                if (isa(ex, InvalidStateException))
                    # InvalidStateException => channel is closed and empty, no more work coming
                    println("Arrow '$(arrow.name)': worker $(id) shutting down.")
                    break;
                else
                    rethrow(ex)
                    # All other exceptions are displayed at the end of run
                end
            end
        end


    end

    function run(arrows; nthreads=Threads.nthreads())
        println("Starting run() with nthreads = $(nthreads)")
        for arrow in arrows
            for id in 1:arrow.max_parallelism
                push!(arrow.worker_tasks, @spawn worker(arrow, id))
            end

            arrow.shutdown_task = @spawn begin
                println("Arrow '$(arrow.name)': Launched shutdown task")
                for w in arrow.worker_tasks
                    wait(w)
                end
                close(arrow.output_channel)
            end
            println("Arrow '$(arrow.name)': All workers have started")
        end
        println("All workers have started")
        try
            Base.exit_on_sigint(false)
            for arrow in arrows
                wait(arrow.shutdown_task)
                println("Arrow '$(arrow.name)': All workers have shut down")
            end
            println("All workers have shut down")
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

    function spin(time_ms)
        s = 0
        for i = 1:time_ms
            s += sum(rand(300,300).^2)
        end
        return s
    end

    function test_source(event, state)
        if state.last_event<state.max_event_count
            state.last_event += 1
            fresh_event = ["emit $(state.last_event) $(spin(100))"]
            return fresh_event
        else
            return :shutdown
        end
    end

    function test_map(event, state)
        push!(event, "map $(spin(500))")
        return event
    end

    function test_reduce(event, state)
        push!(event, "reduce $(spin(200))")
        return event
    end

    mutable struct SourceState
        last_event::Int64
        max_event_count::Int64
    end

    function run_basic_example()
        println("Running basic example")
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        @spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        source = Arrow("source", test_source, SourceState(0, 120), pool, emitted, 1, [], nothing)
        map = Arrow("map", test_map, nothing, emitted, mapped, 4, [], nothing)
        reduce = Arrow("reduce", test_reduce, nothing, mapped, pool, 1, [], nothing)
        topology = [source, map, reduce]
        run(topology)

        println("------------")
        println("Run finished.")

        for arrow in topology
            for (id,task) in enumerate(arrow.worker_tasks)
                if (istaskfailed(task))
                    println("Arrow '$(arrow.name)': worker $(id): $(task.result)")
                else
#                     println("Arrow '$(arrow.name)': worker $(id): Success")
                end
            end
            if (istaskfailed(arrow.shutdown_task))
                println("Arrow '$(arrow.name)': shutdown: $(arrow.shutdown_task.result)")
            else
#                 println("Arrow '$(arrow.name)': shutdown: Success")
            end
        end
    end
end

Engine.run_basic_example()
