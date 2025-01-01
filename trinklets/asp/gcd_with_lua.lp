#script (lua)
clingo = require("clingo")
N = clingo.Number
function gcd(a, b)
    if a.number == 0 then
        return b
    else
        na = a.number
        nb = b.number
        nc = nb % na
        return gcd(N(nc), a)
    end
end
#end.

p(210,213).
p(1365,385).
gcd(X,Y,@gcd(X,Y)) :- p(X,Y).
