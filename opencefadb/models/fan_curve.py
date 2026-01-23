from typing import Union, Sequence, Callable, Optional

import matplotlib.pyplot as plt
import numpy as np
from ontolutils import QUDT_UNIT
from ontolutils.ex import qudt
from ontolutils.ex.sosa import ObservationCollection
from ontolutils.ex.ssn import Observation, Result
from ontolutils.namespacelib import QUDT_KIND
from rdflib.namespace import split_uri
from ssnolib.m4i import NumericalVariable

import opencefadb


def _parse_unit(unit: Union[str, qudt.Unit]) -> str:
    if isinstance(unit, str):
        unit = qudt.Unit.get(unit)
    if unit.symbol is None:
        unit = unit.expand()
    return unit


Selector = Union[str, Callable[[Observation], NumericalVariable]]
LabelResolver = Callable[[NumericalVariable], str]


def resolve_selector(sel: Selector) -> Callable[[Observation], NumericalVariable]:
    if callable(sel):
        return sel
    if isinstance(sel, str):
        return lambda obs: _standard_name_selector(obs, sel)
    raise TypeError(f"Unsupported selector type: {type(sel)}")


def _get_label(variable: NumericalVariable) -> Optional[str]:
    if variable.label is None:
        return None
    return str(variable.label)


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
    LABEL_SELECTION_ORDER = [
        "label",
        "symbol",
        "altLabel",
        "standard_name",
    ]

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


def _standard_name_selector(obs: Observation, standard_name: str) -> Optional[NumericalVariable]:
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


class SemanticOperationPoint:
    EXPECTED_PRESSURE_QUANTITY_KINDS = [
        QUDT_KIND.Pressure,
        QUDT_KIND.StaticPressure,
        QUDT_KIND.TotalPressure,
    ]
    ROTATIONAL_FREQUENCY_QUANTITY_KIND = QUDT_KIND.RotationalVelocity
    VOLUME_FLOW_RATE_QUANTITY_KIND = QUDT_KIND.VolumeFlowRate

    def __init__(self, observation: Observation):
        self.observation = observation

    def scale(
            self,
            reference_rotational_frequency: NumericalVariable,
            keep_other_results:bool=True,
    ) -> "SemanticOperationPoint":
        """Scale the operating point by the provided numerical variable according to the affinity laws.

        Parameters
        ----------
        reference_rotational_frequency : NumericalVariable
            The reference rotational frequency to scale to.
        keep_other_results : bool, optional
            Whether to keep other results that are not scaled, by default True.
        """
        scaled_result_ids = []

        if not isinstance(reference_rotational_frequency, NumericalVariable):
            raise TypeError("reference_rotational_frequency must be a NumericalVariable")
        if not reference_rotational_frequency.is_kind_of_quantity(self.ROTATIONAL_FREQUENCY_QUANTITY_KIND):
            raise ValueError("reference_rotational_frequency must be of kind of quantity "
                             f"{self.ROTATIONAL_FREQUENCY_QUANTITY_KIND}: {reference_rotational_frequency}")

        observation_rotational_frequency = self.observation.get_numerical_variable_by_kind_of_quantity(
            self.ROTATIONAL_FREQUENCY_QUANTITY_KIND
        )
        if len(observation_rotational_frequency) != 1:
            print(observation_rotational_frequency)
            raise ValueError(
                f"Fan curve must have exactly one numerical variable of kind {self.ROTATIONAL_FREQUENCY_QUANTITY_KIND}")
        observation_rotational_frequency = observation_rotational_frequency[0].to_pint()
        for res in self.observation.hasResult:
            nv = res.hasNumericalVariable
            if nv is not None and nv.is_kind_of_quantity(self.ROTATIONAL_FREQUENCY_QUANTITY_KIND):
                scaled_result_ids.append(res.id)
                break

        if "turn" in observation_rotational_frequency._units:
            observation_rotational_frequency_pint = observation_rotational_frequency.to("turns/s")
        else:
            observation_rotational_frequency_pint = observation_rotational_frequency.to("1/s")

        vfr_numerical_variables = self.observation.get_numerical_variable_by_kind_of_quantity(
            self.VOLUME_FLOW_RATE_QUANTITY_KIND
        )
        if len(vfr_numerical_variables) != 1:
            raise ValueError(
                f"Fan curve must have exactly one numerical variable of kind {self.VOLUME_FLOW_RATE_QUANTITY_KIND}")
        for res in self.observation.hasResult:
            nv = res.hasNumericalVariable
            if nv is not None and nv.is_kind_of_quantity(self.ROTATIONAL_FREQUENCY_QUANTITY_KIND):
                scaled_result_ids.append(res.id)
                break

        pressure_numerical_variables = []
        for PKC in self.EXPECTED_PRESSURE_QUANTITY_KINDS:
            _pressure_variable = self.observation.get_numerical_variable_by_kind_of_quantity(
                PKC
            )
            if len(_pressure_variable) > 0:
                pressure_numerical_variables.extend(_pressure_variable)
            elif len(_pressure_variable) == 0:
                # consider units: # TODO this is a workaround for missing kind_of_quantity
                for result in self.observation.hasResult:
                    nv = result.hasNumericalVariable
                    if nv:
                        if isinstance(nv.hasUnit, str):
                            if str(nv.hasUnit) == str(QUDT_UNIT.PA):
                                if nv not in pressure_numerical_variables:
                                    pressure_numerical_variables.append(nv)
                                    scaled_result_ids.append(result.id)
                        elif str(nv.hasUnit.id) == str(QUDT_UNIT.PA):
                            if nv not in pressure_numerical_variables:
                                pressure_numerical_variables.append(nv)
                                scaled_result_ids.append(result.id)

        reference_rotational_frequency_pint = reference_rotational_frequency.to_pint()
        if "turn" in reference_rotational_frequency_pint._units:
            reference_rotational_frequency_pint = reference_rotational_frequency_pint.to("turns/s")
        else:
            reference_rotational_frequency_pint = reference_rotational_frequency_pint.to("1/s")

        scaled_variables = []
        for p in pressure_numerical_variables:
            scaled_pressure_value = p.hasNumericalValue * (
                    reference_rotational_frequency_pint / observation_rotational_frequency_pint) ** 2
            scaled_p = p.model_copy()
            scaled_p.hasNumericalValue = scaled_pressure_value.magnitude
            scaled_variables.append(scaled_p)

        for vfr in vfr_numerical_variables:
            scaled_vfr_value = vfr.hasNumericalValue * (
                    reference_rotational_frequency_pint / observation_rotational_frequency_pint)
            scaled_vfr = vfr.model_copy()
            scaled_vfr.hasNumericalValue = scaled_vfr_value.magnitude
            scaled_variables.append(scaled_vfr)

        scaled_variables.append(
            reference_rotational_frequency.model_copy()
        )

        if keep_other_results:
            for result in self.observation.hasResult:
                if result.id in scaled_result_ids:
                    continue
                nv = result.hasNumericalVariable
                if nv is not None:
                    scaled_variables.append(nv)

        scaled_observation = Observation(
            has_result=[
                Result(
                    has_numerical_variable=n
                )
                for n in scaled_variables
            ]
        )
        return SemanticOperationPoint(scaled_observation)


def get_result_by_standard_name(observation: Observation, name: str=None, iri:str=None):
        """Get all results matching the provided standard name or IRI."""
        if name is None and iri is None:
            raise ValueError("Either name or iri must be provided")
        for result in observation.hasResult:
            nv = result.hasNumericalVariable
            if nv is not None:
                sn = nv.hasStandardName
                if name is not None:
                    if isinstance(sn, str):
                        continue
                    if sn and sn.standardName == name:
                        return result
                elif iri is not None:
                    if isinstance(sn, str):
                        if str(sn) == iri:
                            return result
                    elif sn and str(sn.id) == iri:
                        return result
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
        self.collection: ObservationCollection = collection

    def __len__(self):
        return len(self.collection.hasMember)

    def __repr__(self):
        return f"<SemanticFanCurve with {len(self)} observations>"

    @classmethod
    def from_observations(cls, observations: Sequence[Observation], **kwargs) -> "SemanticFanCurve":
        observations = list(observations)
        feature_of_interests = [
            obs.hasFeatureOfInterest for obs in observations if
            obs.hasFeatureOfInterest is not None
        ]
        # check if they all have the same foi
        if len(set(feature_of_interests)) > 1:
            raise ValueError("All observations must have the same feature of interest")
        foi = feature_of_interests[0] if len(feature_of_interests) > 0 else None
        for k, v in kwargs.copy().items():
            if v is None:
                kwargs.pop(k)
        oc = ObservationCollection(has_member=list(observations), **kwargs)
        if foi:
            oc.hasFeatureOfInterest = foi
        return cls(oc)

    def get_result_by_standard_name(self, name: str=None, iri:str=None):
        """Get all results matching the provided standard name or IRI."""
        if name is None and iri is None:
            raise ValueError("Either name or iri must be provided")
        results = []
        for obs in self.collection.hasMember:
            _res = get_result_by_standard_name(obs, name=name, iri=iri)
            if _res is not None:
                results.append(_res)
        return results

    def get_result_by_kind_of_quantity(self, koq: qudt.QuantityKind=None, iri: str=None):
        """Get all results matching the provided kind of quantity."""
        if koq is None and iri is None:
            raise ValueError("Either koq or iri must be provided")

        results = []
        for obs in self.collection.hasMember:
            for result in obs.hasResult:
                nv = result.hasNumericalVariable
                if nv is not None:
                    if koq is not None:
                        if nv.is_kind_of_quantity(koq):
                            results.append(result)
                    elif iri is not None:
                        if nv.hasKindOfQuantity is not None:
                            if isinstance(nv.hasKindOfQuantity, str):
                                if str(nv.hasKindOfQuantity) == iri:
                                    results.append(result)
                            elif str(nv.hasKindOfQuantity.id) == iri:
                                results.append(result)
        return results

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

    def scale(
            self,
            n: NumericalVariable,
            keep_other_results: bool = True,
    ):
        """Scale the fan curve by the provided numerical variable according to the affinity laws."""
        if len(self) == 0:
            raise ValueError("Cannot scale an empty fan curve.")
        scaled_members = [
            SemanticOperationPoint(observation).scale(n, keep_other_results=keep_other_results).observation for observation in self.collection.hasMember
        ]
        return SemanticFanCurve.from_observations(
            scaled_members,
            id=self.collection.id,
            hasFeatureOfInterest=self.collection.hasFeatureOfInterest,
        )
        # for observation in self.collection.hasMember:
        #     scaled_observations =
        #     for result in obs.hasResult:
        #         nv = result.hasNumericalVariable
        #         if nv is not None:
        #             if nv.has_numerical_value is not None:
        #                 scaled_value = nv.has_numerical_value * n.has_numerical_value
        #             else:
        #                 scaled_value = None
        #             scaled_nv = NumericalVariable(
        #                 id=nv.id,
        #                 label=nv.label,
        #                 hasStandardName=nv.hasStandardName,
        #                 has_numerical_value=scaled_value,
        #                 hasUnit=nv.hasUnit,
        #             )
        #             scaled_result = type(result)(
        #                 id=result.id,
        #                 hasNumericalVariable=scaled_nv,
        #             )
        #             scaled_results.append(scaled_result)
        #         else:
        #             scaled_results.append(result)
        #     scaled_obs = type(obs)(
        #         id=obs.id,
        #         hasResult=scaled_results,
        #         hasFeatureOfInterest=obs.hasFeatureOfInterest,
        #         hadPrimarySource=obs.hadPrimarySource,
        #     )
        #     scaled_members.append(scaled_obs)

    def _get_plotting_data(
            self,
            x: Selector,
            y: Selector,
            xlabel: Union[str, LabelResolver] = None,
            ylabel: Union[str, LabelResolver] = None,
            xsort: bool = True,
            ret_err: bool = False,
            raise_on_no_data_points=True
    ):
        resolved_x, resolved_y = self.get_xy(x, y)

        # strip None values in xs and ys (pair-wise)
        xy_filtered = [(xv, yv) for xv, yv in zip(resolved_x, resolved_y) if xv is not None and yv is not None]
        xs, ys = [t[0].hasNumericalValue for t in xy_filtered], [t[1].hasNumericalValue for t in xy_filtered]
        if len(xy_filtered) == 0:
            if raise_on_no_data_points:
                raise ValueError("No data points could be extracted.")
            else:
                return [], [], None, None, "x / ?", "y / ?"
        if xsort:
            xy = sorted(zip(xs, ys), key=lambda t: t[0])
            xs, ys = [t[0] for t in xy], [t[1] for t in xy]

        if ret_err:
            # hasUncertaintyDeclaration may be null!
            xerr = []
            for t in xy_filtered:
                if t[0].hasUncertaintyDeclaration is not None:
                    xerr.append(t[0].hasUncertaintyDeclaration.has_standard_uncertainty)
                else:
                    xerr.append(np.nan)
            yerr = []
            for t in xy_filtered:
                if t[1].hasUncertaintyDeclaration is not None:
                    yerr.append(t[1].hasUncertaintyDeclaration.has_standard_uncertainty)
                else:
                    yerr.append(np.nan)
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
            raise_on_no_data_points: bool = True,
            **kwargs
    ):
        verbose = kwargs.pop("verbose", None)
        ax = kwargs.pop("ax", None)
        if ax is None:
            ax = plt.gca()

        xs, ys, xerr, yerr, _xlabel, _ylabel = self._get_plotting_data(x, y, xlabel, ylabel, xsort,
                                                                       raise_on_no_data_points=raise_on_no_data_points)

        if ys:
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
            raise_on_no_data_points: bool = True,
            **kwargs
    ):
        verbose = kwargs.pop("verbose", None)
        ax = kwargs.pop("ax", None)
        if ax is None:
            ax = plt.gca()
        opencefadb.opencefa_print("Obtaining plotting data...", verbose=verbose)
        xs, ys, xs_err, ys_err, _xlabel, _ylabel = self._get_plotting_data(x, y, xlabel, ylabel, xsort, ret_err=True,
                                                                           raise_on_no_data_points=raise_on_no_data_points)
        opencefadb.opencefa_print("... done.", verbose=verbose)
        if ys:
            ax.errorbar(xs, ys, xerr=xs_err, yerr=ys_err, **kwargs)
            plt.xlabel(_xlabel)
            plt.ylabel(_ylabel)
        return ax
