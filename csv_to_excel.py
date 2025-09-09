#!/usr/bin/env python3
import csv
import os
import argparse
from typing import List

from openpyxl import Workbook


def write_excel(rows: List[List[str]], headers: List[str], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Articles")
    # Remove default sheet if present
    if wb.worksheets and wb.worksheets[0].title == "Sheet":
        wb.remove(wb.worksheets[0])
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert CSV to Excel and optionally drop the Text column")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=False, help="Output XLSX path (full columns)")
    parser.add_argument(
        "--no-text-output",
        dest="no_text_output",
        required=False,
        help="Output XLSX path without the Text column",
    )
    args = parser.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Identify Text column index (case-sensitive header expected)
    text_idx = None
    try:
        text_idx = headers.index("Text")
    except ValueError:
        text_idx = None

    if args.output:
        write_excel(rows, headers, args.output)

    if args.no_text_output:
        if text_idx is not None:
            headers_no_text = [h for i, h in enumerate(headers) if i != text_idx]
            rows_no_text = [[v for i, v in enumerate(r) if i != text_idx] for r in rows]
        else:
            headers_no_text = headers
            rows_no_text = rows
        write_excel(rows_no_text, headers_no_text, args.no_text_output)


if __name__ == "__main__":
    main()


