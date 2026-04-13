import random
import inspect

txn1 = [
    "begin",
    "select val from t1 where id = 1 for update",
    "update t1 set val = val + 1 where id = 1",
    "select sum(val) from t1",
    "commit"
]

txn2 = [
    "begin",
    "select val from t1 where id > 1 for update",
    "update t1 set val = val + 1 where id = 2",
    "select sum(val) from t1",
    "commit"
]

def make_generator(txn):
    def gen():
        for stmt in txn:
            yield stmt

    # initialize a generator object
    obj = gen()
    return obj


def make_generator_bidi(txn):
    def gen():
        for stmt in txn:
            pushback = yield stmt
            while pushback is not None:
                pushback = yield pushback
    # initialize a generator object
    obj = gen()
    return obj


def interleave():
    gen_list = [
        ("t1", make_generator(txn1)),
        ("t2", make_generator(txn2)),
    ]

    while True:
        if not gen_list:
            break
        txn_name, choose_gen = random.choice(gen_list)
        try:
            stmt = next(choose_gen)
            print(f"{txn_name}: {stmt}")
        except StopIteration:
            gen_list.remove((txn_name, choose_gen))


def interleave2():
    gen_list = [
        ("t1", make_generator_bidi(txn1)),
        ("t2", make_generator_bidi(txn2)),
    ]

    meta_comnds = [
        "sleep(5)",
        "repeat",
        None,
    ]

    counter = 0
    while True:
        if not gen_list:
            break
        txn_name, choose_gen = random.choice(gen_list)
        try:
            stmt = None
            mixin_selected = random.choice(meta_comnds)
            if not mixin_selected or inspect.getgeneratorstate(choose_gen) == inspect.GEN_CREATED:
                stmt = next(choose_gen)
            elif mixin_selected == "repeat":
                stmt = choose_gen.send(f"repeated: {stmt}")
            else:
                stmt = choose_gen.send(f"mixin: {mixin_selected}")

            counter += 1
            print(f"{counter}: {txn_name}: {stmt}")
        except StopIteration:
            gen_list.remove((txn_name, choose_gen))


"""
1. Non-SQL keywords
   - stop tserver non-leader
   - python statements ?
   - Generate query on-the-fly
   - Generate hints
   - Arbitrary sleep
2. [ ] Run against yb and compare output against pg
3. [ ] savepoints and rollbacks
4. [ ] ddls
5. [ ] Isolation modes - RR, RC
   - use explcit locking only for RC
6. Data generation
7. [ ] Read-write patterns
   - Causality
     user 1: say hello 1
     user 2: get msg from 1, if hello then reply "i am good 2"
   - Foreign Keys
   - SQL Bank Txns
"""
