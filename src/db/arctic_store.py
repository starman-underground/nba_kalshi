from __future__ import annotations

import json
from typing import TYPE_CHECKING, Optional, Union

import arcticdb as adb
import numpy as np
import pandas as pd
import polars as pl

from config import DATABASE_URI

if TYPE_CHECKING:
    from arcticdb import Arctic

PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]


def _serialize_cell(value):
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def _collect(df: PolarsFrame) -> pl.DataFrame:
    if isinstance(df, pl.LazyFrame):
        return df.collect(streaming=True)
    return df


def _to_arctic_write_format(df: pl.DataFrame, *, index_col: Optional[str] = None) -> pd.DataFrame:
    """Convert Polars to pandas in a form ArcticDB can persist."""
    out = df.to_pandas()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].astype(object)
        elif out[col].dtype == object:
            out[col] = out[col].map(_serialize_cell).astype(object)
    if index_col is not None:
        out = out.set_index(index_col)
        if isinstance(out.index, pd.Index) and pd.api.types.is_string_dtype(out.index):
            out.index = out.index.astype(object)
    return out


class ArcticStore:
    """Low-level ArcticDB operations."""

    def __init__(self, uri: str = DATABASE_URI):
        self._uri = uri
        self._arctic: Arctic | None = None

    @property
    def arctic(self) -> "Arctic":
        if self._arctic is None:
            self._arctic = adb.Arctic(self._uri)
        return self._arctic

    def get_library(self, name: str, *, create_if_missing: bool = True):
        return self.arctic.get_library(name, create_if_missing=create_if_missing)

    def list_libraries(self) -> list[str]:
        return self.arctic.list_libraries()

    def list_symbols(self, library_name: str) -> list[str]:
        return self.get_library(library_name).list_symbols()

    def write(
        self,
        library_name: str,
        symbol: str,
        df: PolarsFrame,
        *,
        index_col: Optional[str] = None,
    ) -> None:
        data = _to_arctic_write_format(_collect(df), index_col=index_col)
        self.get_library(library_name).write(symbol, data)

    def read(
        self,
        library_name: str,
        symbol: str,
        *,
        as_of: Optional[int] = None,
    ) -> pl.DataFrame:
        return (
            self.get_library(library_name)
            .read(symbol, as_of=as_of, output_format="POLARS")
            .data
        )

    def has_symbol(self, library_name: str, symbol: str) -> bool:
        return self.get_library(library_name).has_symbol(symbol)

    def delete_symbol(self, library_name: str, symbol: str) -> None:
        self.get_library(library_name).delete(symbol)
