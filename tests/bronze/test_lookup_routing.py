import pytest

from opl.bronze.lookup_routing import LOOKUP_SUFFIX, lookup_type_from_filename

# NOTE: lookup_type_from_filename is the pure "spec oracle" for the suffix map;
# the production path uses opl.bronze.autoloader.lookup_type_column (a Column
# expression built from the same LOOKUP_SUFFIX dict, covered in
# tests/bronze/test_autoloader_helpers.py). Keep both in agreement.


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("F.K03200$Z.D60613.CNAECSV", "cnae"),
        ("F.K03200$Z.D60613.MOTICSV", "motivo"),
        ("F.K03200$Z.D60613.MUNICCSV", "municipio"),
        ("F.K03200$Z.D60613.NATJUCSV", "natureza_juridica"),
        ("F.K03200$Z.D60613.PAISCSV", "pais"),
        ("F.K03200$Z.D60613.QUALSCSV", "qualificacao"),
        ("/Volumes/workspace/default/landing/cnpj/2026-06/F.K03200$Z.D60613.PAISCSV", "pais"),
    ],
)
def test_maps_every_landed_lookup(filename, expected):
    assert lookup_type_from_filename(filename) == expected


def test_all_six_suffixes_present():
    assert set(LOOKUP_SUFFIX.values()) == {
        "cnae", "motivo", "municipio", "natureza_juridica", "pais", "qualificacao",
    }


def test_rejects_non_csv_and_unknown():
    with pytest.raises(ValueError):
        lookup_type_from_filename("F.K03200$Z.D60613.ESTABELE0")   # not a *CSV lookup
    with pytest.raises(ValueError):
        lookup_type_from_filename("F.K03200$Z.D60613.XXXXCSV")     # unknown suffix
