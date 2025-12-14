from typing import List, Optional

import matplotlib as mpl
import matplotlib.pyplot as plt
from ontolutils.ex.m4i import NumericalVariable
from ontolutils.ex.qudt import Unit
from ontolutils.ex.qudt.conversion import convert_value_qudt

mpl.rcParams["text.usetex"] = True

FALLBACK_XNAME = "x"
FALLBACK_YNAME = "y"


def _get_unit_str(unit: Unit) -> str:
    unit_str = unit.latex_symbol or None
    if unit_str is None:
        return unit.symbol or unit.label or str(unit.id) or str(unit)
    if unit_str.startswith("$") and unit_str.endswith("$"):
        unit_str = unit_str[1:-1]
    return f"${unit_str}$"


def plot(
        *,
        y: List[NumericalVariable],
        x: Optional[List[NumericalVariable]] = None,
        xlabel=None,
        ylabel=None,
        xunit: Unit = None,
        yunit: Unit = None,
        ax=None,
        use_standard_names_over_labels=True,
        xsort: bool = False,
        **kwargs
):
    if ax is None:
        ax = plt.gca()

    args = []
    kwargs = kwargs or {}

    xlabel = xlabel or FALLBACK_XNAME
    ylabel = ylabel or FALLBACK_YNAME

    xx, yy = None, None
    if x:
        ref_xunit = xunit or x[0].hasUnit

        xx = [convert_value_qudt(_x.hasNumericalValue, _x.hasUnit, ref_xunit) for _x in x]
        if isinstance(ref_xunit, str):
            x_unit_str = ref_xunit
        else:
            x_unit_str = _get_unit_str(ref_xunit)

        if use_standard_names_over_labels:
            xname = x[0].hasStandardName or x[0].label or FALLBACK_XNAME
        else:
            xname = x[0].label or x[0].hasStandardName or FALLBACK_XNAME
        xlabel = rf"{xname} / {x_unit_str}"

    if y:
        ref_yunit = yunit or y[0].hasUnit
        yy = [convert_value_qudt(_y.hasNumericalValue, _y.hasUnit, ref_yunit) for _y in y]
        if isinstance(ref_yunit, str):
            y_unit_str = ref_yunit
        else:
            y_unit_str = _get_unit_str(ref_yunit)

        if use_standard_names_over_labels:
            yname = y[0].hasStandardName or y[0].label or "y"
        else:
            yname = y[0].label or y[0].hasStandardName or "y"
        ylabel = rf"{yname} / {y_unit_str}"

    if x and y and xsort:
        sorted_pairs = sorted(zip(xx, yy), key=lambda pair: pair[0])
        xx, yy = zip(*sorted_pairs)

    if xx:
        args.append(xx)
    if yy:
        args.append(yy)

    ax.plot(*args, **kwargs)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    return ax
