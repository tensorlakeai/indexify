@tensorlake.api()
def gen_seq(n: int):
    return [square(i) for i in range(n)], 5

# series of functions to call 

@tensorlake.function()
def square(x: int):
    return sum(x * x)

@tensorlake.function()
def square1(x: int):
    return sum(x * x + 2)


@tensorlake.function()
def sum(x: int, acc: int, is_last: bool):
    acc = acc + x
    if is_last:
        return sum_everything(acc)
    return acc

@tensorlake.function()
def sum_everything(x: int, acc: int, is_last: bool):
    acc = acc + x
    # is_last doesn't matter since this is the leaf node
    return acc

# result

# sum needs to be called N times as the number of elements in sequence

# ReduceTask(x1)
# ReduceTask(x2)
# ReduceTask(x3)
# Task(x1, acc)
# Task(x2, acc)
# Task(x3, acc)

@tensorlake.function(acc: int)
def sum(x: int, acc: int, is_last: bool):
    return acc + x
