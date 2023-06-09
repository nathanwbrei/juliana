"""
# module Event

- Julia version: 
- Author: nbrei
- Date: 2023-06-06

# Examples

```jldoctest
julia>
```
"""
module Event

struct Parameter
    name::String
    default::String
    value::String
    has_default::Bool
    is_default::Bool
    is_advanced::Bool
end

struct Condition
    name::String
    value::String
end

abstract type Service end



abstract type Factory end

function init!(self::Factory, parameters::Dict{String, Parameter}, services::Dict{String, Service})
    # No-op by default
end

function beginRun!(self::Factory, runNumber::Int64, conditions::Dict{String, Condition})
    # No-op by default
end

function processEvent(self::Factory, event)
    error("process() method not implemented in $(typeof(processor)).")
end

function endRun!(self::Factory)
    # No-op by default
end

function finish!(self::Factory)
    # No-op by default
end

function getData(self::Factory)
    error("getData() method not implemented in $(typeof(processor)).")
end

function clearData(self::Factory)
    error("getData() method not implemented in $(typeof(processor)).")
end



struct Event
    eventNumber::Int64
    runNumber::Int64
    factorySet::Dict{String,Factory}
end

function getFactory(self::Event, collectionName::String)::Factory
    if haskey(self.factorySet, collectionName)
        return get(self.factorySet, collectionName, "")
    else
        return Nothing
    end
end

function getData(self::Event, collectionName::String)
    fac = getFactory(self, collectionName)
    processEvent(fac, self)
    return getData(fac)
end



end