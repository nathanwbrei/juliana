#=
EventTests:
- Julia version: 1.9.0
- Author: nbrei
- Date: 2023-06-08
=#


module EventTests
mutable struct SillyFactory <: Factory
    data::Int64
    param::Int64
    cond::Int64
end

function SillyFactory()
    SillyFactory(-1,0,0)
end

function processEvent(self::Factory, event)
    print("$(typeof(self)): processEvent")
    self.data = 618
end

function getData(self::Factory)
    return self.data
end

function clearData(self::Factory)
    self.data = -1
end


e = Event(0,0,Dict("Silly"=>SillyFactory()))
getData(e, "Silly")
e

end # module EventTests

