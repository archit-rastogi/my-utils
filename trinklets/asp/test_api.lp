#script (python)
import requests
from clingo import Number

def test():
    resp = requests.get("https://release.dev.yugabyte.com/version/released/2.14", timeout=60)
    return Number(resp.status_code)
#end.

api_called(X) :- X = @test.