from abc import abstractmethod
from enum import Enum, auto
from typing import Iterator


class UOM(Enum):
    """
    This is a base class for Enums the enumerate a particular class of unit of measure

    """
    @abstractmethod
    def abbreviation(self) -> str:
        """
        This method provides an abbreviation for a particular u.o.m, e.g. lbs for pounds

        :return: Unit of measure abbreviation
        :rtype: str

        """
        return ' '

    @abstractmethod
    def __iter__(self) -> Iterator:
        ...


class Weight(UOM):
    """
    This Enum contains u.o.m.'s used to measure weight

    """
    POUNDS = auto()
    KILOS = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Weight.POUNDS, Weight.KILOS
        :rtype: Enum value

        """
        for i in range(1, Weight.__len__()+1):
            yield Weight(i)

    def abbreviation(self):
        """
        Provides an abbreviation  for each Weight u.o.m.

        :return: u.o.m. abbreviation
        :rtype: str

        """
        match self:
            case Weight.POUNDS:
                return "lbs"
            case Weight.KILOS:
                return "kg"
            case _:
                return "?"

    def __str__(self) -> str:
        """
        Converts enum to string representation

        :return:  a string containing the name and abbreviation for a u.o.m
        :rtype: str

        """
        return f"{self.name}  {self.abbreviation()}"


class Pressure(UOM):
    """
    This Enum contains u.o.m's used to measure blood pressure

    """
    MM_OF_HG = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Pressure.MM_OF_HG
        :rtype: Enum value

        """
        for i in range(1, Pressure.__len__()+1):
            yield Pressure(i)

    def abbreviation(self):
        """
        Provides an abbreviation  for each Pressure u.o.m.

        :return: u.o.m. abbreviation
        :rtype: str
        """
        return "mm/Hg"

    def __str__(self) -> str:
        """
        Converts enum to string representation

        :return:  a string containing the name and abbreviation for a u.o.m
        :rtype: str

        """
        return f"{self.name}  {self.abbreviation()}"


class Temperature(UOM):
    """
    This Enum contains u.o.m.'s used to measure temperature

    """
    CELSIUS = auto()
    FAHRENHEIT = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Temperature.CELSIUS, Temperature.FAHRENHEIT
        :rtype: Enum value

        """
        for i in range(1, Temperature.__len__()+1):
            yield Temperature(i)

    def abbreviation(self):
        """
        Provides an abbreviation  for each Temperature u.o.m.

        :return: u.o.m. abbreviation
        :rtype: str

        """
        match self:
            case Temperature.CELSIUS:
                return "\u00b0 C"
            case Temperature.FAHRENHEIT:
                return "\u00b0 F"
            case _:
                return "\u00b0 ?"

    def __str__(self) -> str:
        """
        Converts enum to string representation

        :return:  a string containing the name and abbreviation for a u.o.m
        :rtype: str

        """
        return f"{self.name}  {self.abbreviation()}"


class BG(UOM):
    """
    This Enum contains u.o.m.'s used to measure blood glucose levels

    """
    MG_PER_DL = auto()
    MMOL_PER_L = auto()

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields BG.MM_PER_DL, BG.MMOL_PER_L
        :rtype: Enum value

        """
        for i in range(1, BG.__len__()+1):
            yield BG(i)

    def abbreviation(self):
        """
        Provides an abbreviation  for each Blood Glucose u.o.m.

        :return: u.o.m. abbreviation
        :rtype: str

        """
        match self:
            case BG.MG_PER_DL:
                return "mg/dL"
            case BG.MMOL_PER_L:
                return "mmol/L"
            case _:
                return "?"

    def __str__(self) -> str:
        """
        Converts enum to string representation

        :return:  a string containing the name and abbreviation for a u.o.m
        :rtype: str

        """
        return f"{self.name}  {self.abbreviation()}"


class Rate(UOM):
    """
    This Enum contains u.o.m.'s used to measure periodic phenomena such as pulse and respiration

    """
    BPM = auto()

    def abbreviation(self):
        """
        Provides an abbreviation  for each Periodic Rate u.o.m.

        :return: u.o.m. abbreviation
        :rtype str:

        """
        return "b/m"

    def __iter__(self) -> Iterator:
        """
        Overrides the Iterable.__next__ method, yields each of the valid values for this Enum subclass

        :return: yields Rate.BPM
        :rtype: Enum value

        """
        for i in range(1, Rate.__len__()+1):
            yield Rate(i)

    def __str__(self) -> str:
        """
        Converts enum to string representation

        :return:  a string containing the name and abbreviation for a u.o.m
        :rtype: str
        """
        return f"{self.name}  {self.abbreviation()}"


uom_classes = [Weight, Pressure, Temperature, BG, Rate]
"""
A list containing the classes used to record biometric data

:property uom_classes:

"""


uom_class_names = [uc.__name__ for uc in uom_classes]
"""
A list contains the names of classes used to record biometric data

:property uom_class_names:
"""

# TODO: Figure out create uom_map with a list comprehension
uom_map = {}
for cls in uom_classes:
    for uom in cls:
        uom_map[uom.name] = uom
"""
A dict[str, model.uoms.UOM] the provides a mapping between the name property of a UOM derived Enum value and the
corresponding derived Enum

:property uom_map:
"""

