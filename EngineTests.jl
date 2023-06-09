"""
# module EngineTests

- Julia version: 1.9.0
- Author: nbrei
- Date: 2023-06-08

# Examples

```jldoctest
julia>
```
"""
module EngineTests

    include("Engine.jl")
    using .Engine: Arrow,run,graceful_shutdown


    function spin(time_ms)
        s = 0
        for i = 1:time_ms
            s += sum(rand(300,300).^2)
        end
        return s
    end

    function randomly_hang(probability=0.05, time_ms=100000)
        x = rand()
        if x<probability
           spin(time_ms)
        end
        return 0
    end

    function randomly_throw(probability=0.05)
        x = rand()
        if x<probability
            @warn "Randomly throwing!"
            error("Some exception happens!")
        end
        @info "Not randomly throwing"
        return 0
    end

    function test_source(event, state)
        if state.last_event<state.max_event_count
            state.last_event += 1
            fresh_event = ["emit $(state.last_event) $(spin(state.spin_ms))"]
            return fresh_event
        else
            return :shutdown
        end
    end

    mutable struct SourceState
        last_event::Int64
        max_event_count::Int64
        spin_ms::Int64
    end

    function run_basic_example()
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        source = Arrow("source", test_source, SourceState(0, 100, 100), pool, emitted, 1)
        map    = Arrow("map", (event,state)->push!(event, "map $(spin(500))"), nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", (event,state)->push!(event, "reduce $(spin(200))"), nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        Threads.@spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        run(topology)
    end

    function run_interrupted_example()
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        source = Arrow("source", test_source, SourceState(0, 100, 100), pool, emitted, 1)
        map    = Arrow("map", (event,state)->push!(event, "map $(spin(500))"), nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", (event,state)->push!(event, "reduce $(spin(200))"), nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        Threads.@spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        interactive_task = Base.current_task()
        Threads.@spawn begin
            @warn("Triggering graceful shutdown in 5 seconds...")
            sleep(5)
            graceful_shutdown(topology)
        end
        run(topology)
    end


    function run_timeout_example()
        @info("Running timeout example")
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        source = Arrow("source", test_source, SourceState(0, 100, 100), pool, emitted, 1)
        map    = Arrow("map", (event,state)->push!(event, "map $(spin(500)) $(randomly_hang())"), nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", (event,state)->push!(event, "reduce $(spin(200))"), nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        Threads.@spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        run(topology)
    end

    function excepting_map(event, state)
        # We make this be its own function so that we can demonstrate a nice stack trace
        spin(500)
        randomly_throw()
        return push!(event, "map")
    end


    function run_excepting_example()
        @info("Running excepting example")
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        source = Arrow("source", test_source, SourceState(0, 100, 100), pool, emitted, 1)
        map    = Arrow("map", excepting_map, nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", (event,state)->push!(event, "reduce $(spin(200))"), nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        Threads.@spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        run(topology)
    end


    function run_fast_example()
        @info("Running fast example")
        pool = Channel(20)
        emitted = Channel(20)
        mapped = Channel(20)
        source = Arrow("source", test_source, SourceState(0, 10000000, 0), pool, emitted, 1)
        map    = Arrow("map", (event,state)->push!(event, "map"), nothing, emitted, mapped, 4)
        reduce = Arrow("reduce", (event,state)->push!(event, "reduce"), nothing, mapped, pool, 1)
        topology = [source, map, reduce]
        Threads.@spawn begin
            for i in 1:20
                put!(pool, Vector{String}())
            end
        end
        run(topology)
    end

end

import REPL
options = ["Run basic example", "Run interrupted example", "Run timeout example", "Run excepting example", "Run fast example"]
funs = [EngineTests.run_basic_example, EngineTests.run_interrupted_example, EngineTests.run_timeout_example, EngineTests.run_excepting_example, EngineTests.run_fast_example]
menu = REPL.TerminalMenus.RadioMenu(options, pagesize=6, charset=:unicode)
choice = REPL.TerminalMenus.request("What would you like to do?", menu)
funs[choice]()

