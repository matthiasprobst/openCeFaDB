from typing import Union, Sequence, Callable, Optional

import matplotlib.pyplot as plt
from ontolutils.ex import qudt
from ontolutils.ex.ssn import Observation
from rdflib.namespace import split_uri
from ssnolib.m4i import NumericalVariable

from opencefadb.models import ObservationCollection


def _parse_unit(unit: Union[str, qudt.Unit]) -> str:
    if isinstance(unit, str):
        unit = qudt.Unit.get(unit)
    if unit.symbol is None:
        unit = unit.expand()
    return unit


# class SemanticOperatingPoint:
#
#     def __init__(self, x, y, n, attrs=None):
#         self.x = x
#         self.y = y
#         self.n = n
#         self.attrs = attrs or {}
#
#     @classmethod
#     def from_observation(cls, observation: Observation):
#         results = observation.hasResult
#         x = next(r.hasNumericalVariable for r in results if
#                  r.hasNumericalVariable.hasStandardName.standardName == "arithmetic_mean_of_fan_volume_flow_rate")
#         y = next(r.hasNumericalVariable for r in results if
#                  r.hasNumericalVariable.hasStandardName.standardName == "arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet")
#         n = next(r.hasNumericalVariable for r in results if
#                  r.hasNumericalVariable.hasStandardName.standardName == "arithmetic_mean_of_fan_rotational_speed")
#         attrs = observation.model_dump(exclude_none=True)
#         attrs.pop("hasResult", None)
#         return cls(x=x, y=y, n=n, attrs=attrs)
#
#     def plot(self, **kwargs):
#         ax = kwargs.pop("ax", None)
#         if ax is None:
#             ax = plt.gca()
#
#         x_unit = _parse_unit(self.x.hasUnit)
#         y_unit = _parse_unit(self.y.hasUnit)
#         ax.plot(
#             self.x.hasNumericalValue,
#             self.y.hasNumericalValue, marker="o",
#             label=f"n={self.n.hasNumericalValue}"
#         )
#         xlabel = kwargs.pop("xlabel", None)
#         if xlabel is None:
#             xlabel = self.x.label or self.x.altLabel or self.x.hasStandardName.standardName
#
#         ylabel = kwargs.pop("ylabel", None)
#         if ylabel is None:
#             ylabel = self.y.label or self.y.altLabel or self.y.hasStandardName.standardName
#
#         # x_unit_symbol = self.x.hasUnit.symbol or self.x.hasUnit
#         # y_unit_symbol = self.y.hasUnit.symbol or self.x.hasUnit
#         ax.set_xlabel(f"{xlabel} [{x_unit.symbol}]")
#         ax.set_ylabel(f"{ylabel} [{y_unit.symbol}]")
#         return ax


Selector = Union[str, Callable[[Observation], NumericalVariable]]
LabelResolver = Callable[[NumericalVariable], str]


def resolve_selector(sel: Selector) -> Callable[[Observation], NumericalVariable]:
    if callable(sel):
        return sel
    if isinstance(sel, str):
        return lambda obs: standard_name_selector(obs, sel)
    raise TypeError(f"Unsupported selector type: {type(sel)}")


def _get_label(variable: NumericalVariable) -> Optional[str]:
    if variable.label is not None:
        return str(variable.label)
    return None


def _get_alt_label(variable: NumericalVariable) -> Optional[str]:
    if variable.altLabel is None:
        return None
    return str(variable.altLabel)


def _get_symbol(variable: NumericalVariable) -> Optional[str]:
    if variable.hasSymbol is None:
        return None
    return str(variable.hasSymbol)


def _get_standard_name(variable: NumericalVariable) -> Optional[str]:
    if isinstance(variable.hasStandardName, str):
        if variable.hasStandardName.startswith("http"):
            return split_uri(variable.hasStandardName)[-1]
        else:
            return str(variable.hasStandardName)
    elif variable.hasStandardName.standardName is not None:
        return str(variable.hasStandardName.standardName)
    return None


LABEL_SELECTION_MAPPER = {
    "label": _get_label,
    "symbol": _get_symbol,
    "altLabel": _get_alt_label,
    "standard_name": _get_standard_name,
}


class DefaultLabelResolver:
    LABEL_SELECTION_ORDER = {
        "label",
        "symbol",
        "altLabel",
        "standard_name",
    }

    def __call__(self, variable: NumericalVariable) -> str:
        unit = None
        name = None
        for sel in self.LABEL_SELECTION_ORDER:
            lso = LABEL_SELECTION_MAPPER.get(sel, None)
            if lso is None:
                raise ValueError(f"Unknown label selection option: {sel}")
            name = lso(variable)
            if name is not None:
                break

        if variable.hasUnit is not None:
            if isinstance(variable.hasUnit, str):
                try:
                    qunit = _parse_unit(variable.hasUnit)
                    unit = qunit.symbol
                except Exception:
                    unit = str(variable.hasUnit)
            elif isinstance(variable.hasUnit, qudt.Unit):
                qunit = variable.hasUnit.expand()
                unit = qunit.symbol
            # elif variable.hasUnit.unit is not None:
            #     unit = str(variable.hasUnit.unit)

        if name is None:
            name = "?"
        if unit is None:
            unit = "?"
        return f"{name} / {unit}"


def standard_name_selector(obs: Observation, standard_name: str) -> Optional[NumericalVariable]:
    wanted = str(standard_name).strip()
    is_iri = wanted.startswith("http://") or wanted.startswith("https://")

    if is_iri:
        for result in obs.hasResult:
            nv = result.hasNumericalVariable
            if nv:
                sn = nv.hasStandardName
                if isinstance(sn, str):
                    if str(sn) == wanted:
                        return nv
                elif sn and str(sn.id) == wanted:
                    return nv
    else:
        for result in obs.hasResult:
            nv = result.hasNumericalVariable
            if nv:
                sn = nv.hasStandardName
                if isinstance(sn, str):
                    continue
                if sn and sn.standardName == wanted:
                    return nv
    return None


class SemanticFanCurve:
    """
    A view on an ObservationCollection that enables plotting based on two
    selected result variables. The collection is the single source of truth.
    """

    def __init__(self, collection: ObservationCollection):
        if collection is None:
            raise ValueError("collection must not be None")
        if not isinstance(collection, ObservationCollection):
            raise TypeError("collection must be an ObservationCollection")
        self.collection = collection

    def __len__(self):
        return len(self.collection.hasMember)

    @classmethod
    def from_observations(cls, observations: Sequence[Observation], **kwargs) -> "SemanticFanCurve":
        oc = ObservationCollection(hasMember=list(observations), **kwargs)
        return cls(oc)

    def get_xy(self, x: Selector, y: Selector):
        fx = resolve_selector(x)
        fy = resolve_selector(y)

        xs, ys = [], []
        for obs in self.collection.hasMember:
            try:
                xs.append(fx(obs))
                ys.append(fy(obs))
            except KeyError:
                # in der Toolbox: entweder skip + log, oder strict fail
                continue
        return xs, ys

    def serialize(self, format: str = "turtle", **kwargs) -> str:
        return self.collection.serialize(format=format, **kwargs)

    def _get_plotting_data(
            self, x: Selector, y: Selector,
            xlabel: Union[str, LabelResolver] = None,
            ylabel: Union[str, LabelResolver] = None,
            xsort: bool = True,
            ret_err: bool = False):
        resolved_x, resolved_y = self.get_xy(x, y)

        # strip None values in xs and ys (pair-wise)
        xy_filtered = [(xv, yv) for xv, yv in zip(resolved_x, resolved_y) if xv is not None and yv is not None]
        xs, ys = [t[0].hasNumericalValue for t in xy_filtered], [t[1].hasNumericalValue for t in xy_filtered]
        if len(xy_filtered) == 0:
            raise ValueError("No data points could be extracted.")
        if xsort:
            xy = sorted(zip(xs, ys), key=lambda t: t[0])
            xs, ys = [t[0] for t in xy], [t[1] for t in xy]

        if ret_err:
            xerr, yerr = [t[0].hasUncertaintyDeclaration.has_standard_uncertainty for t in xy_filtered], [
                t[1].hasUncertaintyDeclaration.has_standard_uncertainty for t in xy_filtered]
        else:
            xerr, yerr = None, None

        _xlabel = "x / ?"
        if xlabel is None:
            _xlabels = [DefaultLabelResolver()(it) for it in resolved_x if it is not None]
            for it in _xlabels:
                if it is not None:
                    _xlabel = it
                    break
        else:
            if isinstance(xlabel, str):
                _xlabel = xlabel
            else:
                _xlabel = "x / ?"

        _ylabel = "y / ?"
        if ylabel is None:
            _ylabels = [DefaultLabelResolver()(it) for it in resolved_y if it is not None]
            for it in _ylabels:
                if it is not None:
                    _ylabel = it
                    break
        else:
            if isinstance(ylabel, str):
                _ylabel = ylabel
            else:
                _ylabel = "y / ?"

        return xs, ys, xerr, yerr, _xlabel, _ylabel

    def plot(
            self,
            x: Selector,
            y: Selector,
            xlabel: Union[str, LabelResolver] = None,
            ylabel: Union[str, LabelResolver] = None,
            xsort: bool = True,
            **kwargs
    ):
        ax = kwargs.pop("ax", None)
        if ax is None:
            ax = plt.gca()

        xs, ys, xerr, yerr, _xlabel, _ylabel = self._get_plotting_data(x, y, xlabel, ylabel, xsort)

        ax.plot(xs, ys, **kwargs)
        plt.xlabel(_xlabel)
        plt.ylabel(_ylabel)
        return ax

    def errorbar(
            self,
            x: Selector,
            y: Selector,
            xlabel: Union[str, LabelResolver] = None,
            ylabel: Union[str, LabelResolver] = None,
            xsort: bool = True,
            **kwargs
    ):
        ax = kwargs.pop("ax", None)
        if ax is None:
            ax = plt.gca()
        xs, ys, xs_err, ys_err, _xlabel, _ylabel = self._get_plotting_data(x, y, xlabel, ylabel, xsort, ret_err=True)
        ax.errorbar(xs, ys, xerr=xs_err, yerr=ys_err, **kwargs)
        plt.xlabel(_xlabel)
        plt.ylabel(_ylabel)
        return ax
