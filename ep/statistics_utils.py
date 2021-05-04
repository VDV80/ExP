from typing import List


def errors_calc(i: List[List[float]]):
    return map(
        lambda x: abs(x[0] - x[1]), i
    )


def mean_abs_error_calc(i: List[List[float]]):
    return sum(
        map(
            lambda x: abs(x), errors_calc(i)
        )
    ) / len(i)


def mean_error_calc(i: List[List[float]]):
    return sum(errors_calc(i)) / len(i)


def mean_sq_error_calc(i: List[List[float]]):
    """
    not using this one, just for the record
    :param i:
    :return:
    """
    return sum(
        map(lambda x: x ** 2, errors_calc(i))
    ) / len(i)


def confidence_boundary_95prc_calc(i:  List[List[float]]):
    return 1.96 * (mean_sq_error_calc(i) ** 0.5)


def get_all_statistics():
    return (
        ("Avg Error", mean_error_calc),
        ("Mean ABS Error", mean_abs_error_calc),
        ("MSE", mean_sq_error_calc),
        ("CI 95% (1-side range)", confidence_boundary_95prc_calc),
    )






