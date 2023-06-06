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

    pool = Channel(10)
    emitted = Channel(10)

    function run(event_source, event_processor)
        nthreads = Threads.nthreads()
        println("Starting run() with nthreads = $(nthreads)")
        for i in 1:4
            println("pool()")
            put!(pool, ["pool"])
        end
        src_task = @spawn event_source(pool, emitted)
        println("Submitted src_task")

        proc_tasks = []
        for i in 1:nthreads
            push!(proc_tasks, @spawn event_processor(emitted, pool))
            println("Submitted process_task")
        end
        for i in 1:nthreads
            wait(proc_tasks[i])
        end
        wait(src_task)
        println("All threads have joined")
    end

    function test_source(pool, emitted)
        println("Entering test_source")
        for i in 1:10
            event = take!(pool)
            push!(event, "emit event nr $(i)")
            println("emit event nr $(i) on thread $(Threads.threadid())")
            put!(emitted, event)
        end
        for i in 1:Threads.nthreads()
            put!(emitted, :shutdown)
            println("Source: Emitting shutdown signal")
        end
        println("Source is shutting down, finished broadcasting the shutdown signal")
    end

    function test_proc(emitted, pool)
        println("Entering test_proc")
        shutdown = false
        while !shutdown
            event = take!(emitted)
            if event == :shutdown
                println("process: Shutdown received on thread $(Threads.threadid())")
                shutdown = true
            else
                push!(event, "process")
                println("process(): $(event) on thread $(Threads.threadid())")
                empty!(event)
            end
            put!(pool, event)
        end
        println("Processor is shutting down")
    end

    function run_basic_example()
        Base.exit_on_sigint(false)
        try
            run(test_source, test_proc)
        catch ex
            if isa(ex, InterruptException)
                println("Interrupted! Exiting")
#                 println("Pool contains: ")
#                 for i in pool
#                     @show i
#                 end
                println("emitted contains: ")
                for i in emitted
                    @show i
                end


            end
        end

    end

end


Engine.run_basic_example()
