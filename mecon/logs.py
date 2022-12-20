import logging
from functools import wraps
from time import time

VERBOSITY = 1

# https://www.codegrepper.com/code-examples/python/python+print+error+in+red
def colored(r, g, b, text):
    return "\033[38;2;{};{};{}m{} \033[38;2;255;255;255m".format(r, g, b, text)


def log(*args, verbosity=1, rgb=(255, 255, 255)):
    if verbosity <= VERBOSITY:
        colored_args = tuple([colored(*rgb, arg) for arg in args])
        print(*colored_args)


def log_system(*args, **kwargs):
    log(*args, **kwargs, rgb=(50, 200, 200))


def log_error(*args, **kwargs):
    log(*args, **kwargs, rgb=(250, 50, 50))

def log_disk_io(*args, **kwargs):
    log(*args, **kwargs, rgb=(250, 250, 250))

def log_html(*args, **kwargs):
    log(*args, **kwargs, rgb=(50, 200, 50))

def log_calculation(*args, **kwargs):
    log(*args, **kwargs, rgb=(200, 50, 200))




_CNT = 0


def func_execution_logging(_func):
    @wraps(_func)
    def wrapper(*args, **kwargs):

        start_time = time()
        _func_name = f"{_func.__module__}.{_func.__qualname__}"

        global _CNT
        padding = '--' * _CNT
        _CNT += 1
        try:
            log_system(f"{padding}=> Running {_func_name}...")
            res = _func(*args, **kwargs)
        except Exception as e:
            _CNT -= 1
            log_error(f"Function {_func_name} raised exception: '{e}'.")
            raise

        elapsed_time = time() - start_time
        log_system(
            f"<={padding} Function {_func_name} successfully finished in {elapsed_time:.2f} seconds."
        )
        _CNT -= 1
        return res

    return wrapper


