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

    function run(event_source, event_processor)
        pool = Channel(10)
        for i in 1:4
            println("pool()")
            put!(pool, ["pool"])
        end
        emitted = Channel(10)
        src_task = @async event_source(pool, emitted)
        println("Submitted src_task")
        process_task = @async event_processor(emitted, pool)
        println("Submitted process_task")
        wait(src_task)
        wait(process_task)
    end

    function test_source(pool, emitted)
        println("Entering test_source")
        for i in 1:10
            event = take!(pool)
            push!(event, "emit event nr $(i)")
            println("emit event nr $(i)")
            put!(emitted, event)
        end
    end

    function test_proc(emitted, pool)
        println("Entering test_proc")
        for i in 1:10
            event = take!(emitted)
            push!(event, "process")
            println("process(): $(event)")
            empty!(event)
            put!(pool, event)
        end
    end

    function run_basic_example()
        run(test_source, test_proc)
    end

end


Engine.run_basic_example()
