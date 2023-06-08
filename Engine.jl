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
    import Base.Threads.@spawn
    import Dates
    import Printf.@printf

    mutable struct Arrow
        name::String
        execute::Function
        state::Any
        input_channel::Channel
        output_channel::Channel
        max_parallelism::Int64
        processed_count::Threads.Atomic{Int64}
        worker_tasks::Vector{Task}
        shutdown_task::Union{Task,Nothing}

        function Arrow(name, execute, state, input_channel, output_channel, max_parallelism)
            new(name, execute, state, input_channel, output_channel, max_parallelism, Threads.Atomic{Int64}(0), [], nothing)
        end
    end

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
#                 println("Arrow '$(arrow.name)': worker $(id), thread $(Threads.threadid()): $(result)")
                if arrow.output_channel != nothing
                    put!(arrow.output_channel, result)
                end
                Threads.atomic_add!(arrow.processed_count, 1)
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
                println("Arrow '$(arrow.name)': All workers have shut down")
            end
            println("Arrow '$(arrow.name)': All workers have started")
        end
        println("All workers have started")
        try
            Base.exit_on_sigint(false)
            run_start_time = Dates.now()
            last_processed_count = 0
            result = :timedout
            while result != :ok
                start_time = Dates.now()
                result = Base.timedwait(()->istaskdone(arrows[end].shutdown_task), 1.0; pollint=0.1)
                finish_time = Dates.now()
                elapsed_time_total = finish_time - run_start_time
                elapsed_time_delta = finish_time - start_time
                processed_count_total = arrows[end].processed_count[]
                processed_count_delta = processed_count_total - last_processed_count
                last_processed_count = processed_count_total
                @printf("Processed %d events @ avg=%.2f Hz, inst=%.2f Hz\n", processed_count_total, processed_count_total*1000/elapsed_time_total.value, processed_count_delta*1000/elapsed_time_delta.value)
            end

            println("All workers have shut down")
        catch ex
            @show ex
            if isa(ex, InterruptException)
                println("Interrupted! Exiting")
            else
                rethrow(ex)
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
            fresh_event = ["emit $(state.last_event) $(spin(0))"]
            return fresh_event
        else
            return :shutdown
        end
    end

    function test_map(event, state)
        push!(event, "map $(spin(0))")
        return event
    end

    function test_reduce(event, state)
        push!(event, "reduce $(spin(0))")
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
        source = Arrow("source", test_source, SourceState(0, 5000000), pool, emitted, 1)
        map    = Arrow("map", test_map, nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", test_reduce, nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        run(topology)

        println("------------")
        println("Run finished")
        println("------------")

        for arrow in topology
            println("$(arrow.name): Processed $(arrow.processed_count[])")
            for (id,task) in enumerate(arrow.worker_tasks)
                if (istaskfailed(task))
                    println("Arrow '$(arrow.name)': worker $(id): $(task.result)")
                else
#                     println("$(arrow.name):$(id): Success")
                end
            end
            if (istaskfailed(arrow.shutdown_task))
                println("Arrow '$(arrow.name)': shutdown: $(arrow.shutdown_task.result)")
            else
#                 println("$(arrow.name):shutdown: Success")
            end
        end
    end
end

Engine.run_basic_example()


# Still want:
# - Clean interrupt
# - Ticker
# - Timeout
# - Split

