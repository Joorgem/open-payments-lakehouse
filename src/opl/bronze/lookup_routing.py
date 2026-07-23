"""Route an RFB lookup file to its lookup_type by the inner-file suffix.
Filenames look like `F.K03200$Z.D60613.CNAECSV`; the token before the trailing
`CSV` identifies the lookup (verified against the six landed files)."""
from __future__ import annotations

import os

LOOKUP_SUFFIX: dict[str, str] = {
    "CNAE": "cnae",
    "MOTI": "motivo",
    "MUNIC": "municipio",
    "NATJU": "natureza_juridica",
    "PAIS": "pais",
    "QUALS": "qualificacao",
}


def lookup_type_from_filename(filename: str) -> str:
    ext = os.path.basename(filename).rsplit(".", 1)[-1]  # "CNAECSV"
    if not ext.endswith("CSV") or len(ext) <= 3:
        raise ValueError(f"not a lookup CSV filename: {filename!r}")
    code = ext[:-3]                                       # "CNAE"
    try:
        return LOOKUP_SUFFIX[code]
    except KeyError:
        raise ValueError(f"unknown lookup suffix {code!r} in {filename!r}") from None
