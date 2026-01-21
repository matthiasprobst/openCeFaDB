"""

converts physical variables of pressure and volume flow rate to dimensionless numbers psi and phi and wise versa
from dimensionless to physical variables

"""
from typing import Union

import numpy as np
import xarray as xr


def psi(dp: Union[float, np.ndarray],
        n: Union[float, np.ndarray],
        rho: Union[float, np.ndarray],
        D: float) -> Union[float, np.ndarray]:
    """Head coefficient

    Parameters
    ----------
    dp: `float` or `np.ndarray[1d]`
        pressure head [Pa]
    n: `float` or `np.ndarray[1d]`
        revolution speed [rev/s]
    rho: `float` or `np.ndarray[1d]`
        Density of fluid in [kg/m^3]
    D: `float`
        Diameter [m] of the vane
    """
    u2 = np.pi * n * D
    return dp / (rho / 2 * u2 ** 2)


def psi2dptot(psi, n, rho, D):
    """Calculates pressure increase from pressure coefficient

    Parameters
    ----------
    psi: `float` or `np.ndarray[1d]`
        pressure coefficient [-]
    n: `float` or `np.ndarray[1d]`
        revolution speed [rev/s]
    rho: `float` or `np.ndarray[1d]`
        Density of fluid in [kg/m^3]
    D: `float`
        Diameter [m] of the vane
    """
    return psi * (0.5 * np.pi ** 2 * rho * D ** 2 * n ** 2)


def phi(vfr, n, D):
    """Flow coefficient

    Parameters
    ----------
    vfr: `float` or `np.ndarray[1d]`
        volume flow rate in [m^3/s]
    n: `float` or `np.ndarray[1d]`
        revolution speed [rev/s]
    D: `float`
        Diameter [m] of the vane
    """
    u2 = np.pi * n * D
    return vfr / (np.pi / 4 * D ** 2 * u2)


def phi2vfr(phi, n, D):
    """Calculates volume flow rate from flow coefficient

    Parameters
    ----------
    phi: `float` or `np.ndarray[1d]`
        flow coefficient [-]
    n: `float` or `np.ndarray[1d]`
        revolution speed [rev/s]
    D: `float`
        Diameter [m] of the vane
    """
    return phi * (np.pi ** 2 * D ** 3 * n) / 4


def compute_from_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Compute the dimless dataset. Required data_vars: n, rho, D.
    Only phi and psi are computed, the rest is dropped"""

    if 'n' not in ds:
        raise KeyError('Missing revolution speed data_var!')

    n = ds.n.pint.quantify().pint.to('1/s').pint.dequantify()
    rho = ds.rho

    D = ds.D.pint.quantify().pint.to('m').pint.dequantify()
    _psi = psi(ds.dp_tt.pint.quantify().pint.to('Pa').pint.dequantify(),
               n=n,
               rho=rho,
               D=D)
    _phi = phi(ds.vfr.pint.quantify().pint.to('m^3/s').pint.dequantify(),
               n=n,
               D=D)
    _phi.attrs = {'long_name': 'flow coefficient',
                  'units': '',
                  'standard_name': 'fan_flow_coefficient'}
    _psi.attrs = {'long_name': 'pressure coefficient',
                  'units': '',
                  'standard_name': 'fan_pressure_coefficient'}
    dimless_ds = xr.Dataset(
        data_vars={'psi': _psi, 'phi': _phi}
    )
    dimless_ds.attrs = ds.attrs
    # dimless_ds.parent = ds
    return dimless_ds
