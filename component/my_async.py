from threading import Thread


def async_call(f):
    def wrapper(*args, **kwargs):
        Thread(target=f, args=args, kwargs=kwargs).start()

    return wrapper
