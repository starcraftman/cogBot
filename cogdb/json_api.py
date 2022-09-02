"""
Module to parse and import data from new json source.
"""
POWER_ID_MAP = {
    100000: "Aisling Duval",
    100010: "Edmund Mahon",
    100020: "Arissa Lavigny-Duval",
    100040: "Felicia Winters",
    100050: "Denton Patreus",
    100060: "Zachary Hudson",
    100070: "Li Yong-Rui",
    100080: "Zemina Torval",
    100090: "Pranav Antal",
    100100: "Archon Delaine",
    100120: "Yuri Grom",
}


def load_base_json(base):
    """ Load the base json and parse all information from it.

    Args:
        base: The base json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    systems_by_power = {}
    powers = base['powers']
    for bundle in powers:
        power_name = POWER_ID_MAP[bundle['powerId']]
        sys_state = bundle['state']

        # TODO: Design db objects to hook
        for sys_addr, data in bundle['systemAddr'].items():
            system = {
                'id': sys_addr,
                'state': sys_state,
                'income': data['income'],
                'tAgainst': data['thrAgainst'],
                'tFor': data['thrFor'],
                'upkeep': data['upkeepCurrent'],
                'upkeep_default': data['upkeepDefault'],
            }

            try:
                systems_by_power[power_name] += [system]
            except KeyError:
                systems_by_power[power_name] = [system]


    return systems_by_power


def load_refined_json(refined):
    """ Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    preps = {}

    for bundle in refined["preparation"]:
        power_name = POWER_ID_MAP[bundle['power_id']]
        preps[power_name] = [{'id': id, 'total': total} for id, total in bundle['rankedSystems']]

    return preps
