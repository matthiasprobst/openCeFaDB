from ontolutils.ex.ssn import Observation

from ontolutils.namespacelib import QUDT_KIND

EXPECTED_PRESSURE_QUANTITY_KINDS = [
    QUDT_KIND.Pressure,
    QUDT_KIND.StaticPressure,
    QUDT_KIND.TotalPressure,
]


def is_operating_point(observation: Observation, verbose: bool = False) -> bool:
    """Check if an observation is a fan operating point.

    Args:
        observation (Observation): The observation to check.
        the observation must have minimum three numerical variables with the following
        quantity kinds:
            - air_flow_rate
            - static_pressure_difference
            - rotational_speed
    Returns:
        bool: True if the observation is a fan operating point, False otherwise.
    """
    has_results = observation.has_result
    if not has_results or len(has_results) < 3:
        return False
    has_vfr_kind = False
    has_dp_kind = False
    has_n_kind = False
    for result in has_results:
        num_var = result.has_numerical_variable
        if num_var.is_kind_of_quantity(QUDT_KIND.VolumeFlowRate):
            has_vfr_kind = True
            continue
        if num_var.is_kind_of_quantity(QUDT_KIND.RotationalVelocity):
            has_n_kind = True
            continue
        for EPQK in EXPECTED_PRESSURE_QUANTITY_KINDS:
            if num_var.is_kind_of_quantity(EPQK):
                has_dp_kind = True
                break
    if verbose:
        if not has_vfr_kind:
            print("Missing Volume Flow Rate kind")
        if not has_dp_kind:
            print("Missing Pressure kind")
        if not has_n_kind:
            print("Missing Rotational Velocity kind")
    return has_vfr_kind and has_dp_kind and has_n_kind
