from typing import List, Union, Tuple

import matplotlib.pyplot as plt
import rdflib
from ontolutils import Thing, urirefs, namespaces
from pydantic import Field

from .utils import remove_none


@namespaces(
    ex="http://example.org/",
)
@urirefs(
    Value='ex:Value',
    label='ex:label',  # closeMatch rdfs:label
    value='ex:value',  # closeMatch hdf:value
    units="ex:hasUnit",  # closeMatch m4i:hasUnit
    hasStandardName="ex:hasStandardName",  # closeMatch ssno:hasStandardName
    symbol="ex:hasSymbol"
)
class Value(Thing):
    """A data point with metadata such as value, units, standard name and symbol."""
    label: str = Field(default=None)
    hasStandardName: str = Field(default=None)
    value: float
    units: str = Field(default=None)
    symbol: str = Field(default=None)


@namespaces(ex="https://example.org/")
@urirefs(
    DataSet='ex:DataSet,',
    data='ex:data'
)
class DataSet(Thing):
    data: List[Value] = Field(alias='values')

    def plot(self,
             x_standard_name: Union[rdflib.URIRef, str] = None,
             y_standard_name: Union[rdflib.URIRef, str] = None,
             x_id=None,
             y_id=None,
             marker: str = "o",
             color: str = "k",
             label: str = None,
             ax=None):
        """Plotting based on either x_standard_name and y_standard_name or x_id and y.

        Hence, either provide x_standard_name and y_standard_name or x_id and y_id.
        """
        # check input:
        if (x_standard_name is not None and y_standard_name is not None) and (x_id is not None or y_id is not None):
            raise ValueError("Either provide x_standard_name and y_standard_name or x_id and y_id")
        x = next((v for v in self.data if str(v.standardName) == str(x_standard_name) or str(v.id) == str(x_id)), None)
        if x is None:
            raise ValueError(f"Value for x with URI '{x}' not found in DataSeries.")
        y = next((v for v in self.data if str(v.standardName) == str(y_standard_name) or str(v.id) == str(y_id)), None)
        if y is None:
            raise ValueError(f"Value for y with URI '{y}' not found in DataSeries.")

        ax = ax or plt.gca()
        ax.plot(
            x.value,
            y.value,
            marker=marker,
            color=color,
            label=label
        )
        # ax.set_xlabel(f"{self.coordinates.label} [{self.coordinates.units}]")
        # ax.set_ylabel(f"{self.data.label} [{self.data.units}]")
        return ax


@namespaces(ex="https://example.org/")
@urirefs(
    DataSeries='ex:DataSeries,',
    datasets='ex:datasets'
)
class DataSeries(Thing):
    """Operation point of a fan"""
    datasets: List[DataSet] = Field(alias='datasets')

    def plot(
            self,
            x_standard_name: Union[rdflib.URIRef, str] = None,
            y_standard_name: Union[rdflib.URIRef, str] = None,
            x_id=None,
            y_id=None,
            x_sort: bool = True,
            label: str = None,
            marker: str = "o",
            color: str = "k",
            ax=None
    ):
        """Plotting based on either x_standard_name and y_standard_name or x_id and y.

        Hence, either provide x_standard_name and y_standard_name or x_id and y_id.
        """
        # check input:
        if (x_standard_name is not None and y_standard_name is not None) and (x_id is not None or y_id is not None):
            raise ValueError("Either provide x_standard_name and y_standard_name or x_id and y_id")

        def _parse(_dataset, _standard_name, _id):
            _value = next(
                (v for v in _dataset.data if str(v.standardName) == str(_standard_name) or str(v.id) == str(_id)),
                None)
            return _value

        x = [_parse(d, x_standard_name, x_id) for d in self.datasets]
        y = [_parse(d, y_standard_name, y_id) for d in self.datasets]

        xx = [_x.value for _x in x]
        yy = [_y.value for _y in y]
        if x_sort:
            xx, yy = sort_by_first(xx, yy)

        ax = ax or plt.gca()
        ax.plot(
            xx,
            yy,
            marker=marker,
            color=color,
            label=label
        )
        if x[0].label:
            xlabel = x[0].label
        elif x[0].hasStandardName:
            xlabel = x[0].hasStandardName
        else:
            xlabel = "x"
        if y[0].label:
            ylabel = y[0].label
        elif y[0].hasStandardName:
            ylabel = y[0].hasStandardName
        else:
            ylabel = "y"
        ax.set_xlabel(f"{xlabel} [{x[0].units}]")
        ax.set_ylabel(f"{ylabel} [{y[0].units}]")
        return ax

    def to_xarray_dataarray(self):
        import xarray as xr
        label = str(self.coordinates.label)
        da = xr.DataArray(
            data=[self.data.value],
            coords={label: [self.coordinates.value]},
            dims=[str(self.coordinates.label)],
            name=label,
            attrs={
                "standard_name": self.data.hasStandardName,
                "units": self.data.units,
                "symbol": self.data.symbol
            }
        )
        return da

    @classmethod
    def from_xarray_dataarray(cls, da: "xr.DataArray"):
        coord_value = da.coords[da.dims[0]].values.item()
        data_value = da.values.item()

        _coordinate = dict(
            label=da.dims[0],
            value=coord_value,
            units=da.attrs.get("units", None),
            standardName=da.attrs.get("standard_name", None),
            symbol=da.attrs.get("symbol", None)
        )
        _data = dict(
            label=da.name,
            value=data_value,
            units=da.attrs.get("units", None),
            standardName=da.attrs.get("standard_name", None),
            symbol=da.attrs.get("symbol", None)
        )
        dp = cls(
            coordinates=Value.model_validate(remove_none(_coordinate)),
            data=Value.model_validate(remove_none(_data))
        )
        return dp


def sort_by_first(x: List[float], y: List[float], reverse: bool = False) -> Tuple[List[float], List[float]]:
    """
    Sortiert beide Listen nach den Werten von `x`.
    Gibt neue Listen zurück; original unverändert.
    """
    if len(x) != len(y):
        raise ValueError("Listen müssen die gleiche Länge haben.")
    pairs = sorted(zip(x, y), key=lambda t: t[0], reverse=reverse)
    if not pairs:
        return [], []
    xs, ys = zip(*pairs)
    return list(xs), list(ys)
