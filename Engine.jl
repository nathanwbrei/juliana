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
    import REPL

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
        @debug("Arrow '$(arrow.name)': Launched worker $(id)");
        while true
            if arrow.input_channel == nothing
                event = ()
            else
                try
                    event = take!(arrow.input_channel)
                catch
                    @debug("Arrow '$(arrow.name)': worker $(id) shutting down due to closed input channel.")
                    break;
                end
            end
            task_local_storage(:start_time, Dates.now())
            result = arrow.execute(event, arrow.state)
            task_local_storage(:start_time, 0)  # Tell timeout watchdog that execute() finished
            if (result == :shutdown)
                @debug("Arrow '$(arrow.name)': worker $(id) shutting down due to end-of-stream.")
                break;
            end
            @debug("Arrow '$(arrow.name)': worker $(id), thread $(Threads.threadid()): $(result)")
            if arrow.output_channel != nothing
                try
                    put!(arrow.output_channel, result)
                catch
                    @debug("Arrow '$(arrow.name)': worker $(id) shutting down due to closed output channel.")
                    break;
                end
            end
            Threads.atomic_add!(arrow.processed_count, 1)
        end
    end

    function check_for_timeout(arrows, timeout_duration)
        for arrow in arrows
            for (worker_id,task) in enumerate(arrow.worker_tasks)
                if task.storage != nothing
                    start_time = get(task.storage, :start_time, 0)
                    if start_time != 0
                        execution_duration = Dates.now() - start_time
                        if execution_duration > timeout_duration
                            @error("Timeout detected", execution_duration, arrow.name, worker_id, task)
                            return true
                            # Ideally, we could do `Base.throwto(task, InterruptException)`, however there are two problems
                            # 1. I'm 80% sure this requires the hanging task to yield (because Julia uses fibers, not green threads)
                            # 2. throwto() doesn't support multithreading ("LoadError: cannot switch to task running on another thread")
                        end
                    end
                end
            end
        end
        return false
    end

    function check_for_failed_tasks(arrows)
        found_failed = false
        for arrow in arrows
            for (worker_id,task) in enumerate(arrow.worker_tasks)
                if istaskfailed(task)
                    found_failed = true
                    @error("Failed task detected", arrow.name, worker_id, task)
                end
            end
        end
        return found_failed
    end

    function graceful_shutdown(arrows)
        @warn("Starting graceful shutdown")
        close(arrows[1].output_channel)
        # This looks really weird, but the worker will shut down if its output channel
        # is closed (what else can it do, after all?) If we were to shut down the input channel
        # instead, this would cause problems because it would have to drain the event pool,
        # and would prematurely shut off the farthest downstream arrow. The only other option
        # is to have a shutdown_request flag on each arrow, which I find dissatisfying based
        # off of doing exactly that in JANA2. On the other hand, we may eventually want to support
        # properly pausing a running topology for the sake of debugging, instead of hacking it using
        # JEventProcessors and backpressure.
    end

    function run(arrows; nthreads=Threads.nthreads(), show_ticker=true, timeout_duration=Dates.Second(5), timeout_warmup_duration=Dates.Second(10), exit_on_timeout=true)
        @info("Welcome to the Juliana event reconstruction framework!")
        @info("Starting run()", nthreads, show_ticker, timeout_duration, timeout_warmup_duration)
        for arrow in arrows
            for id in 1:arrow.max_parallelism
                push!(arrow.worker_tasks, @spawn worker(arrow, id))
            end

            arrow.shutdown_task = @spawn begin
                @debug("Arrow '$(arrow.name)': Launched shutdown task")
                for w in arrow.worker_tasks
                    wait(w)
                end
                close(arrow.output_channel)
                @debug("Arrow '$(arrow.name)': All workers have shut down")
            end
            @debug("Arrow '$(arrow.name)': All workers have started")
        end
        @debug("All workers have started")

        Base.exit_on_sigint(false)
        run_start_time = Dates.now()
        last_processed_count = 0
        result = :timedout
        while result != :ok
            try
                while result != :ok
                    start_time = Dates.now()

                    # Check for failed tasks first. This way, we can distinguish between excepted and timedout tasks
                    if check_for_failed_tasks(arrows)
                        exit(1)
                    end

                    # Check for timeout. We do this on the interactive/ticker thread instead of a dedicated
                    # timeout supervisor task for two reasons:
                    # 1. It makes life slightly easier for the task scheduler, and doesn't interfere with real work
                    # 2. We don't need to signal to the timeout supervisor when run() has finished
                    if (timeout_duration != 0)
                        if (start_time - run_start_time) > timeout_warmup_duration
                            if check_for_timeout(arrows, timeout_duration)
                                if exit_on_timeout
                                    exit(1)
                                else
                                    throw(InterruptException)
                                    # I'm not sure what anyone could realistically do with the InterruptException.
                                    # They can't kill the offending task or trigger the shutdown logic
                                    # that is waiting on the task. The caller to run() would be left with
                                    # a pile of tasks that can't finish. The only option I see right now is to
                                    # forcibly shut down the topology by closing all channels and calling all
                                    # arrow finalizers, though we'd still be left with hanging tasks that
                                    # can always corrupt or data-race the finalizers
                                 end
                            end
                        end
                    end

                    # Print the processing ticker.
                    if show_ticker
                        result = Base.timedwait(()->istaskdone(arrows[end].shutdown_task), 1.0; pollint=0.1)
                        finish_time = Dates.now()
                        elapsed_time_total = finish_time - run_start_time
                        elapsed_time_delta = finish_time - start_time
                        processed_count_total = arrows[end].processed_count[]
                        processed_count_delta = processed_count_total - last_processed_count
                        last_processed_count = processed_count_total
                        avg_rate_hz = round(processed_count_total*1000/elapsed_time_total.value; sigdigits=3)
                        inst_rate_hz = round(processed_count_delta*1000/elapsed_time_delta.value; sigdigits=3)
                        @info("Processed $(processed_count_total) events @ avg = $(avg_rate_hz) Hz, inst = $(inst_rate_hz) Hz\n")
    #                   @info("Event processing in progress", processed_events_count=processed_count_total, avg_rate_hz, inst_rate_hz)
                    end
                end
            catch ex
                if isa(ex, InterruptException)
                    options = ["Continue", "Graceful shutdown", "Hard shutdown"]
                    menu = REPL.TerminalMenus.RadioMenu(options, pagesize=6, charset=:unicode)
                    choice = REPL.TerminalMenus.request("\nHow would you like to proceed?", menu)
                    if choice == 2
                        graceful_shutdown(arrows)
                    elseif choice == 3
                        # Hard shutdown
                        @warn("Hard shutdown")
                        exit(1)
                    end
                else
                    rethrow(ex)
                end
            end
        end
        elapsed_time = round(Dates.now() - run_start_time, Dates.Second)
        processed_counts = Dict{String, Int64}()
        for arrow in arrows
            processed_counts[arrow.name] = arrow.processed_count[]
        end
        @info("All workers have shut down successfully", elapsed_time, processed_counts)
    end

end


# Still want:
# - Split

