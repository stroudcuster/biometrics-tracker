import re

bp_re = re.compile('"*(\d{2,3})/(\d{2,3})([\s\w])*"*')


def decode_bloodpressure(bp_str: str) -> tuple[int, int, str]:
    """
    Decodes systolic and diastolic integer values from the provided string, which are expected to be in the form
    <systolic>/<diastolic> with an UOM optionally appended

    :param bp_str: blood pressure in string form
    :type bp_str: str
    :return: the systolic and diastolic values
    :rtype: tuplec[int, int]

    """
    match = bp_re.match(bp_str.strip())
    uom_str: str = ''
    if match is None:
        return 0, 0, ''
    groups = match.groups()
    if len(groups) > 2:
        uom_str = groups[2]
    return int(groups[0]), int(groups[1]), uom_str
