# TODO finish logger
def show_func_called(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug("%s is called" % func.__name__)
            logger.debug("parameter is : ")
            logger.debug(args)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def check_params_dict(func):
    def wrapper(a):
        if isinstance(a, dict):
            return func(a)
        elif not a:
            logger.warning("传入参数为空.")
        else:
            logger.warning("传入参数的类型为：%s" % type(a))
            return
    return wrapper
