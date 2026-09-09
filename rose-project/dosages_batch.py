#!/usr/bin/env python3
"""Batch version of dosages_python_version.ipynb.

Runs the dosage analysis over EVERY workbook in raw_data/ instead of one
hardcoded NAME, so adding a month is a matter of dropping the .xlsx in and
re-running rather than editing a filename.

Per workbook it writes three files, all named after the source workbook so
nothing overwrites anything:

    data/<name>.csv           the decrypted export, as-is
    output/<name>_totals.csv  the totals table, for pasting into Excel
    output/<name>_totals.png  the same table as an image, for a deck or email

The analysis itself — the drug schema, the dose resolution order, the summary
table — is unchanged from the notebook. The notebook's DEBUGGING section
(Dose Resolution, Dose Provenance) is not carried over; run the notebook when
you need those diagnostics.

Usage (from any directory — the data folders are found relative to this file):
    export XAPHERESIS_PASSWORD='...'      # optional; prompts if unset
    python3 dosages_batch.py
    python3 dosages_batch.py --list-values

Needs the pyenv 3.11.2 interpreter (the one the notebook uses); the system
python3 has no msoffcrypto.
"""

import argparse
import getpass
import io
import os
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")           # no display when run as a script

import matplotlib.pyplot as plt
import msoffcrypto
import pandas as pd


# Folders are resolved relative to this script, not to the shell's working
# directory, so the script runs the same from anywhere. The --raw-dir/--data-dir/
# --out-dir overrides are still taken as given, relative to where you ran it.
PROJECT_DIR = Path(__file__).resolve().parent


# ─────────────────────────────────────────────────────────────────────────────
# Drug schema
# ─────────────────────────────────────────────────────────────────────────────

# Resolution order applied to the concentrates below. The measure is stock drawn,
# not drug delivered: a dose pulled from stock is consumed whether it is fully
# given, partly given, or discarded. So `ordered` wins, `order name` supplies the
# absolute amount for per-kg orders, and `administered` is only a last-resort
# witness that something was pulled when nothing else is recorded.
STOCK_ORDER = ["ordered", "order name", "administered", "default"]

# Albumin is dispensed as whole bottles and its default_dose IS one bottle, so a
# partial administered volume would understate the draw — 85 mL charted against a
# 12.5 g bottle is still a 12.5 g draw. Albumin therefore skips `administered`.
ALBUMIN_ORDER = ["ordered", "order name", "default"]

DRUG_SCHEMA = {
    # ── Albumin ──────────────────────────────────────────────────────────────
    "Albumin 5%": {
        "patterns": [r"albumin human 5 %"],
        "match_column": "Order Name",
        "target_unit": "g",
        "unit_conversions": {
            "g":  1.0,
            "mL": 0.05,   # 5% solution = 50 mg/mL = 0.05 g/mL
        },
        "dose_from_name_pattern": r"([\d\.]+)\s*(g|mL)(?:\s|$)",
        "resolution_order": ALBUMIN_ORDER,
        "default_dose": {"amount": 12.5, "unit": "g"},   # one bottle = a real draw
    },
    "Albumin 25%": {
        "patterns": [r"albumin human 25 %"],
        "match_column": "Order Name",
        "target_unit": "g",
        "unit_conversions": {
            "g":  1.0,
            "mL": 0.25,   # 25% solution = 250 mg/mL = 0.25 g/mL
        },
        "dose_from_name_pattern": r"([\d\.]+)\s*(g|mL)(?:\s|$)",
        "resolution_order": ALBUMIN_ORDER,
        "default_dose": {"amount": 12.5, "unit": "g"},   # one bottle = a real draw
    },

    # ── Prothrombin Complex Concentrates ─────────────────────────────────────
    # default_dose stays None for every product below: a placeholder makes a row
    # look resolved, whereas None leaves it out of the total as EXCLUDED.
    "Balfaxar (4F-PCC)": {
        "patterns": [r"balfaxar", r"prothrombin complex human-lans"],
        "match_column": "Order Name",
        "target_unit": "Units",
        "unit_conversions": {
            "Units": 1.0,
        },
        # dose appears before volume, e.g. "1,000 Units 40 mL infusion"
        "dose_from_name_pattern": r"([\d,]+)\s*(Units)",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },
    "Kcentra (4F-PCC)": {
        "patterns": [r"kcentra"],
        "match_column": "Order Name",
        "target_unit": "Units",
        "unit_conversions": {
            "Units": 1.0,
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(Units)",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },

    # ── Antithrombin ─────────────────────────────────────────────────────────
    "Antithrombin III (Thrombate)": {
        "patterns": [r"antithrombin"],
        "match_column": "Order Name",
        "target_unit": "Units",
        "unit_conversions": {
            "Units": 1.0,
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(Units)\s*$",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },

    # ── Antihemophilic Factors ───────────────────────────────────────────────
    "Antihemophilic Factor-VWF (Alphanate)": {
        "patterns": [r"alphanate"],
        "match_column": "Order Name",
        "target_unit": "Units",
        # NOTE: VWF:RCo Units and Int'l Units are summed as if interchangeable.
        # They are different potency measures — see the caveat in the schema notes.
        "unit_conversions": {
            "VWF:RCo Units": 1.0,
            "Int'l Units":   1.0,
            "Units":         1.0,
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(VWF:RCo Units|Int'l Units|Units)\s*$",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },
    "Antihemophilic Factor rAHF-PFM (Advate)": {
        "patterns": [r"rahf-pfm", r"advate"],
        "match_column": "Order Name",
        "target_unit": "Units",
        "unit_conversions": {
            "Units": 1.0,
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(Units)\s*$",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },

    # ── Coagulation Factors ──────────────────────────────────────────────────
    "Coagulation Factor VIIa (NovoSeven)": {
        "patterns": [r"coagulation factor viia"],
        "match_column": "Order Name",
        "target_unit": "mg",
        "unit_conversions": {
            "mg":  1.0,
            "mcg": 0.001,   # 1 mcg = 0.001 mg
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(mcg|mg)\s*$",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "mg"},
    },
    "Coagulation Factor IX (Benefix)": {
        "patterns": [r"coagulation factor ix"],
        "match_column": "Order Name",
        "target_unit": "Units",
        "unit_conversions": {
            "Units": 1.0,
        },
        "dose_from_name_pattern": r"([\d,]+)\s*(Units)\s*$",
        "resolution_order": STOCK_ORDER,
        "default_dose": {"amount": None, "unit": "Units"},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Core functions
# ─────────────────────────────────────────────────────────────────────────────

def classify_row(row, schema):
    """Return the schema key matching this row, or None if no pattern matches."""
    for drug_type, config in schema.items():
        col = config.get("match_column", "Order Name")
        val = str(row.get(col) or "")
        if any(re.search(p, val, re.IGNORECASE) for p in config["patterns"]):
            return drug_type
    return None


def apply_schema(df, schema):
    """Add a 'drug_type' column classifying each row according to the schema."""
    df = df.copy()
    df["drug_type"] = df.apply(lambda row: classify_row(row, schema), axis=1)
    return df


def _convert_dose(amount, unit, config):
    """Convert (amount, unit) to target_unit using config's unit_conversions.
    Returns NaN if unit is not in unit_conversions or amount is NaN."""
    factor = config["unit_conversions"].get(unit)
    if factor is None or pd.isna(amount):
        return float("nan")
    return amount * factor


# The two column pairs a resolution step can read from.
COLUMN_STEPS = {
    "ordered":      ("Ordered Dose Amount", "Ordered Dose Unit"),
    "administered": ("Administered Dose Amount", "Administered Dose Unit"),
}


def _resolve_dose(row, config, with_source=False):
    """Resolve a row's stock draw by walking config['resolution_order'].

    Steps:
        'ordered' / 'administered'  read that column pair
        'order name'                parse the dose out of Order Name
        'default'                   fall back to default_dose

    A /kg or /hr unit is a rate, not an amount, and is skipped — this export has
    no patient weight to apply it to. The absolute amount for those rows lives in
    the Order Name, which the next step picks up.

    A charted administered value of 0 is also skipped: it means the dose never
    reached the patient, which says nothing about stock, since the draw already
    happened. Letting it through would zero out a real draw.

    Returns (amount, unit), or (amount, unit, source) when with_source is True.
    Both are nan/None if nothing resolves.
    """
    conversions = config["unit_conversions"]

    def out(amount, unit, source):
        return (amount, unit, source) if with_source else (amount, unit)

    for step in config.get("resolution_order", STOCK_ORDER):
        if step in COLUMN_STEPS:
            amount_col, unit_col = COLUMN_STEPS[step]
            amount = row.get(amount_col)
            unit = str(row.get(unit_col) or "")
            if unit.endswith("/kg") or unit.endswith("/hr"):
                continue
            if step == "administered" and amount == 0:
                continue
            if not pd.isna(amount) and unit in conversions:
                return out(float(amount), unit, step)

        elif step == "order name":
            pattern = config.get("dose_from_name_pattern")
            if pattern:
                m = re.search(pattern, str(row.get("Order Name") or ""), re.IGNORECASE)
                if m:
                    try:
                        return out(float(m.group(1).replace(",", "")), m.group(2), "order name")
                    except (ValueError, IndexError):
                        pass

        elif step == "default":
            default = config.get("default_dose", {})
            if default.get("amount") is not None:
                return out(float(default["amount"]), default["unit"], "default")

    return out(float("nan"), None, "unresolved")


def get_total_amounts(df, schema):
    """Return a DataFrame with total stock drawn per drug type.

    Dose per row is resolved via _resolve_dose, walking each entry's
    resolution_order, then converted to target_unit.

    Columns:
        total_amount    – sum in target_unit
        target_unit     – unit total_amount is expressed in
        dose_count      – rows matched to this drug type
        excluded_doses  – rows where dose could not be resolved/converted
    """
    rows = []
    for drug_type, config in schema.items():
        subset = df[df["drug_type"] == drug_type].copy()

        def resolve_and_convert(row, cfg=config):
            amount, unit = _resolve_dose(row, cfg)
            return _convert_dose(amount, unit, cfg)

        subset["_converted"] = subset.apply(resolve_and_convert, axis=1)
        rows.append({
            "drug_type":      drug_type,
            "target_unit":    config["target_unit"],
            "total_amount":   subset["_converted"].sum(),
            "dose_count":     len(subset),
            "excluded_doses": int(subset["_converted"].isna().sum()),
        })
    return pd.DataFrame(rows).set_index("drug_type")


def render_table(rows, headers, aligns=None):
    """Render rows of strings as a box-drawn table with grid lines.

    aligns is a per-column format spec ('<' left, '>' right); defaults to left.
    """
    aligns = aligns or ["<"] * len(headers)
    widths = [
        max(len(str(cell)) for cell in (header, *(r[i] for r in rows)))
        for i, header in enumerate(headers)
    ]

    def rule(left, mid, right):
        return left + mid.join("─" * (w + 2) for w in widths) + right

    def line(cells):
        body = " │ ".join(f"{str(c):{a}{w}}" for c, a, w in zip(cells, aligns, widths))
        return f"│ {body} │"

    out = [rule("┌", "┬", "┐"), line(headers), rule("╞", "╪", "╡")]
    for i, row in enumerate(rows):
        if i:
            out.append(rule("├", "┼", "┤"))
        out.append(line(row))
    out.append(rule("└", "┴", "┘"))
    return "\n".join(out)


# ─────────────────────────────────────────────────────────────────────────────
# Loading
# ─────────────────────────────────────────────────────────────────────────────

def find_workbooks(raw_dir):
    """Return every .xlsx in raw_dir, sorted by name.

    Skips Excel's '~$' lock files and dotfiles, which are not real workbooks and
    would blow up read_excel.
    """
    workbooks = []
    for path in sorted(raw_dir.glob("*.xlsx")):
        if path.name.startswith("~$") or path.name.startswith("."):
            continue
        workbooks.append(path)
    return workbooks


def load_workbook(path, password):
    """Decrypt path if it is encrypted, and return it as a DataFrame.

    The export puts ten rows of report header above the real column names, hence
    skiprows=10. Raises on a bad password rather than returning a garbled frame.
    """
    with open(path, "rb") as file:
        office_file = msoffcrypto.OfficeFile(file)

        if not office_file.is_encrypted():
            return pd.read_excel(path, skiprows=10)

        decrypted_workbook = io.BytesIO()
        office_file.load_key(password=password, verify_password=True)
        office_file.decrypt(decrypted_workbook)

    return pd.read_excel(decrypted_workbook, skiprows=10)


def get_password():
    """Password from the environment if set, otherwise prompt once for all files.

    To skip the prompt, run this in the terminal first:
        export XAPHERESIS_PASSWORD='...'
    .strip() guards against a trailing newline from a copy-paste or $(cat ...).
    """
    password = os.environ.get("XAPHERESIS_PASSWORD") or getpass.getpass("Workbook password: ")
    return password.strip()


def list_values(df):
    """Print the distinct generic names and order names in one workbook.

    The scratch listings from the notebook's setup section, kept behind
    --list-values because they run to hundreds of lines per workbook.
    """
    print("\nAll types of Generic")
    generics = df["Slices by Component Simple Generic Name"].unique()
    for value in generics:
        print(value)
    print("Total:", len(generics))

    print("\nAll types of Order Names")
    order_names = df["Order Name"].unique()
    for value in order_names:
        print(value)
    print("Total:", len(order_names))


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────

def analyse_workbook(name, df, out_dir):
    """Run the analysis for one workbook and write its CSV and PNG to out_dir.

    Returns the summary DataFrame, or None if nothing in the workbook matched
    the schema — with no classified rows there is no total to report and the
    coverage footnote has no denominator.
    """
    df_classified = apply_schema(df, DRUG_SCHEMA)

    # Rows that didn't match any schema entry
    unmatched = df_classified[df_classified["drug_type"].isna()]
    if not unmatched.empty:
        print(f"⚠  {len(unmatched)} unmatched rows:")
        print(unmatched[["Order Name", "Ordered Dose Amount", "Ordered Dose Unit"]].value_counts().to_string())
        print()

    amounts = get_total_amounts(df_classified, DRUG_SCHEMA)
    administered = amounts[amounts["dose_count"] > 0]

    if administered.empty:
        print("⚠  no rows matched the drug schema — no totals written for this workbook.")
        return None

    # ── Summary table ────────────────────────────────────────────────────────
    # Display frame: thousands separators throughout, so every number reads the
    # same way in the terminal table, the PNG, and the CSV.
    summary = pd.DataFrame({
        "drug": administered.index,
        "doses": [f"{int(n):,}" for n in administered["dose_count"]],
        "amount drawn": [
            f"{int(total):,} {unit}" if pd.notna(total) else f"n/a {unit}"
            for total, unit in zip(administered["total_amount"], administered["target_unit"])
        ],
    })
    print(render_table(
        summary.values.tolist(),
        list(summary.columns),
        aligns=["<", ">", ">"],
    ))

    # Rows whose dose couldn't be resolved contribute nothing to the totals above
    incomplete = administered[administered["excluded_doses"] > 0]
    if not incomplete.empty:
        print("\n⚠  totals exclude unresolved doses:")
        for drug_type, row in incomplete.iterrows():
            print(f"   {drug_type}: {int(row['excluded_doses'])} of {int(row['dose_count'])} doses had no resolvable amount")

    # ── Client-facing copy ───────────────────────────────────────────────────
    # Named after the source workbook, so every file in raw_data/ writes its own
    # pair of outputs rather than overwriting the last one.
    csv_path = out_dir / f"{name}_totals.csv"
    png_path = out_dir / f"{name}_totals.png"

    # Export carries the formatted strings AND the raw numbers — the commas make
    # the figures readable, the numeric columns keep them summable in Excel.
    export = pd.DataFrame({
        "drug":         administered.index,
        "doses":        administered["dose_count"].to_numpy(),
        "amount drawn": summary["amount drawn"].to_numpy(),
        "amount":       administered["total_amount"].round(2).to_numpy(),
        "unit":         administered["target_unit"].to_numpy(),
    })
    export.to_csv(csv_path, index=False)

    # Coverage figure for the footnote.
    _sources = [_resolve_dose(r, DRUG_SCHEMA[r["drug_type"]], with_source=True)[2]
                for _, r in df_classified.iterrows() if pd.notna(r["drug_type"])]
    _strong = sum(1 for s in _sources if s in ("ordered", "order name"))
    _period = f'{df["Start Date"].min():%d %b %Y} – {df["End Date"].max():%d %b %Y}'

    fig, ax = plt.subplots(figsize=(9.0, 1.15 + 0.30 * len(summary)), dpi=200)
    ax.axis("off")
    tbl = ax.table(cellText=summary.values.tolist(),
                   colLabels=["Drug", "Doses", "Amount Drawn"],
                   colWidths=[0.58, 0.14, 0.28], cellLoc="right", loc="upper center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.5)

    for (r, c), cell in tbl.get_celld().items():
        cell.set_linewidth(0.6)
        cell.set_edgecolor("#dadfe3")
        if r == 0:                                    # header band
            cell.set_facecolor("#2f4858")
            cell.set_text_props(color="white", weight="bold")
            cell.set_height(cell.get_height() * 1.15)
        else:                                         # zebra striping
            cell.set_facecolor("#ffffff" if r % 2 else "#f3f6f8")
        if c == 0:                                    # drug names read better left
            cell.get_text().set_horizontalalignment("left")
            cell.PAD = 0.04

    ax.set_title("Drug Utilization — Stock Drawn", fontsize=15, weight="bold",
                 loc="left", pad=30, color="#1b2a33")
    ax.text(0, 1.045, f"{_period}   ·   {len(df):,} administrations",
            transform=ax.transAxes, fontsize=9.5, color="#5b6b76")
    fig.text(0.045, 0.005,
             "Measure: quantity drawn from stock (ordered), not quantity administered.\n"
             f"{_strong:,} of {len(_sources):,} doses ({_strong / len(_sources):.0%})"
             " resolved from recorded order data.",
             fontsize=8, color="#6b7a85", va="bottom")

    fig.savefig(png_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)          # a batch run would otherwise hold every figure open
    print(f"\nSaved {png_path} and {csv_path}")
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-dir", type=Path, default=PROJECT_DIR / "raw_data",
                        help="folder of source .xlsx workbooks (default: <script folder>/raw_data)")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_DIR / "data",
                        help="where the decrypted .csv exports go (default: <script folder>/data)")
    parser.add_argument("--out-dir", type=Path, default=PROJECT_DIR / "output",
                        help="where the totals .csv and .png go (default: <script folder>/output)")
    parser.add_argument("--list-values", action="store_true",
                        help="also print every distinct generic name and order name")
    args = parser.parse_args()

    workbooks = find_workbooks(args.raw_dir)
    if not workbooks:
        print(f"No .xlsx files found in {args.raw_dir}/", file=sys.stderr)
        return 1

    args.data_dir.mkdir(exist_ok=True)
    args.out_dir.mkdir(exist_ok=True)

    print(f"Found {len(workbooks)} spreadsheets in {args.raw_dir}/: "
          f"{', '.join(p.stem for p in workbooks)}\n")
    password = get_password()

    failed = []
    for path in workbooks:
        name = path.stem
        print("=" * 78)
        print(name)
        print("=" * 78)

        # One bad workbook shouldn't cost the whole batch — report it, keep going,
        # and let the exit code carry the failure.
        try:
            df = load_workbook(path, password)
            print(f"Loaded {len(df)} administrations from {path.name}\n")

            df.to_csv(args.data_dir / f"{name}.csv", index=False)

            if args.list_values:
                list_values(df)
                print()

            analyse_workbook(name, df, args.out_dir)
        except Exception as exc:
            print(f"✗  {name} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            failed.append(name)
        print()

    done = len(workbooks) - len(failed)
    print("=" * 78)
    print(f"Finished: {done} of {len(workbooks)} workbooks processed.")
    if failed:
        print(f"Failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
