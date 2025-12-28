from typing import Union, Sequence, Callable, Optional

import matplotlib.pyplot as plt
import rdflib
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


def _get_symbol(variable: NumericalVariable) -> Optional[str]:
    if variable.label is not None:
        return str(variable.hasSymbol)
    return None


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
    "standard_name": _get_standard_name,
}


class DefaultLabelResolver:
    LABEL_SELECTION_ORDER = {
        "label",
        "symbol",
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
            elif variable.hasUnit.unit is not None:
                unit = str(variable.hasUnit.unit)

        if name is None:
            name = "?"
        if unit is None:
            return "?"
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

    def plot(self,
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
        resolved_x, resolved_y = self.get_xy(x, y)

        # strip None values in xs and ys (pair-wise)
        xy_filtered = [(xv, yv) for xv, yv in zip(resolved_x, resolved_y) if xv is not None and yv is not None]
        xs, ys = [t[0].hasNumericalValue for t in xy_filtered], [t[1].hasNumericalValue for t in xy_filtered]

        if xsort:
            xy = sorted(zip(xs, ys), key=lambda t: t[0])
            xs, ys = [t[0] for t in xy], [t[1] for t in xy]

        if xlabel is None:
            _xlabel = DefaultLabelResolver()(xy_filtered[0][0])
        else:
            if isinstance(xlabel, str):
                _xlabel = xlabel
            elif callable(xlabel):
                _xlabel = xlabel(xy_filtered[0][0])
            else:
                _xlabel = "x / ?"

        if ylabel is None:
            _ylabel = DefaultLabelResolver()(xy_filtered[0][1])
        else:
            if isinstance(ylabel, str):
                _ylabel = ylabel
            elif callable(ylabel):
                _ylabel = ylabel(xy_filtered[0][1])
            else:
                _ylabel = "y / ?"

        ax.plot(xs, ys, **kwargs)
        plt.xlabel(_xlabel)
        plt.ylabel(_ylabel)
    # def __init__(self, x: List[SemanticOperatingPoint], y: List[SemanticOperatingPoint], n: NumericalVariable):
    #     # if len(x) == 0 or len(y) == 0:
    #     #     raise ValueError("x and y must not be empty")
    #     # if not isinstance(n, NumericalVariable):
    #     #     raise TypeError("n must be a NumericalVariable")
    #     # if not (len(x) == len(y)):
    #     #     raise ValueError("x and y must have the same length")
    #     self.x = x
    #     self.y = y
    #     self.n = n
    #     self.collection = None
    #
    # @classmethod
    # def from_observations(
    #         cls,
    #         observations: List[Observation],
    #         id: Union[str, rdflib.URIRef] = None,
    #         hasFeatureOfInterest: Union[str, rdflib.URIRef] = None,
    #         type: Union[str, rdflib.URIRef] = None,
    #         **kwargs
    # ) -> "SemanticFanCurve":
    #     observation_collection = ObservationCollection(
    #         id=id,
    #         hasFeatureOfInterest=hasFeatureOfInterest,
    #         hasMember=observations,
    #         type=type,
    #         **kwargs
    #     )
    #     return cls.from_observation_collection(
    #         observation_collection
    #     )
    #
    # @classmethod
    # def from_observation_collection(
    #         cls,
    #         observation_collection: ObservationCollection
    # ) -> "SemanticFanCurve":
    #     sfc = SemanticFanCurve([], [], 1)
    #     sfc.collection = observation_collection
    #     return sfc
    #
    # def serialize(self, format: str = "turtle", **kwargs) -> str:
    #     return self.collection.serialize(format=format, **kwargs)
    #
    # @classmethod
    # def from_operating_points(cls,
    #                           operating_points: List[SemanticOperatingPoint],
    #                           n_target: NumericalVariable) -> "SemanticFanCurve":
    #     if not isinstance(n_target, NumericalVariable):
    #         raise TypeError("n_target must be a NumericalVariable")
    #
    #     n_target_xarray = n_target.to_xarray()
    #     n_target_hz = n_target_xarray.pint.quantify().pint.to("1/s").pint.magnitude
    #     with xr.set_options(keep_attrs=True):
    #         for op in operating_points:
    #             op_x_xarr = op.x.to_xarray()
    #             op_y_xarr = op.y.to_xarray()
    #             op_n_xarr = op.n.to_xarray()
    #
    #             n_x_hz = op_n_xarr.pint.quantify().pint.to("1/s").pint.magnitude
    #             scale_factor = n_target_hz / n_x_hz
    #
    #             op_x_xarr_q = op_x_xarr.pint.quantify()
    #             op_y_xarr_q = op_y_xarr.pint.quantify()
    #
    #             x_array_scaled = (op_x_xarr_q * scale_factor).pint.dequantify()
    #             y_array_scaled = (op_y_xarr_q * (scale_factor ** 2)).pint.dequantify()
    #
    #             op.x = op.x.__class__.from_xarray(x_array_scaled)
    #             op.y = op.y.__class__.from_xarray(y_array_scaled)
    #
    #     # sort:
    #     new_x = [op.x for op in operating_points]
    #     new_y = [op.y for op in operating_points]
    #
    #     paired = sorted(zip(new_x, new_y), key=lambda p: p[0].hasNumericalValue)
    #
    #     if paired:
    #         new_x_sorted, new_y_sorted = map(list, zip(*paired))
    #     else:
    #         new_x_sorted, new_y_sorted = [], []
    #
    #     return SemanticFanCurve(
    #         x=new_x_sorted,
    #         y=new_y_sorted,
    #         n=n_target
    #     )
    #
    # def plot(self, **kwargs):
    #     ax = kwargs.pop("ax", None)
    #     if ax is None:
    #         ax = plt.gca()
    #
    #     xlabel = kwargs.pop("xlabel", None)
    #     if xlabel is None:
    #         xlabel = self.x[0].label or self.x[0].altLabel or self.x[0].hasStandardName.standardName
    #     ylabel = kwargs.pop("ylabel", None)
    #     if ylabel is None:
    #         ylabel = self.y[0].label or self.y[0].altLabel or self.y[0].hasStandardName.standardName
    #
    #     xx = [x.hasNumericalValue for x in self.x]
    #     yy = [y.hasNumericalValue for y in self.y]
    #
    #     plt.plot(xx, yy, **kwargs)
    #
    #     x_unit = _parse_unit(self.x[0].hasUnit)
    #     y_unit = _parse_unit(self.y[0].hasUnit)
    #
    #     ax.set_xlabel(f"{xlabel} [{x_unit.symbol}]")
    #     ax.set_ylabel(f"{ylabel} [{y_unit.symbol}]")
    #     return ax
