# ERA issue 94: date decoding and release review

The legacy Hippo/Elephant merge binds character Excel date tokens to R `Date`
columns. The resulting mixed representation shifts Hippo serials by 25,569 days
and loses valid Elephant ISO values during JSON serialization. The correction
normalizes eight identified calendar fields before binding, checks both cohorts'
dates remain exact, and verifies calendar-day JSON round trips.

Related issue: https://github.com/ERAgriculture/era-program/issues/94

## Code and tests

- `R/utils/era_dates.R`: Excel origin `1899-12-30`, strict DMY/ISO decoding,
  unresolved-token evidence and guarded date binding. Two-digit years, incomplete
  dates and invalid calendar days are not inferred. Existing typed dates remain
  exact, including fractional days in RData; JSON retains calendar-day precision.
- `R/compile/merge_mj_ie.R`: decode six `Data.Out` planting/harvest/sample fields,
  `Fert.Method.F.Date` and `Res.Method.M.Date`; write row-linked raw-date audit;
  correct the IE type comparison, undefined save path and broken trailing read.
- `tests/test_issue94_dates.R`: 27 assertions across eight regression tests.
- `tests/replay_issue94_dates.R`: full offline merge, save and date round-trip
  checks using explicitly selected source files and an empty output directory.
- `scripts/stage_issue94_json.py`: SHA-pinned, date-only historical JSON staging;
  exact decimal preservation and row-identity checks. No network/publishing code.
- `tests/test_issue94_staging.py`: four tests covering decimal preservation,
  source-pin failure, row-identity failure and overwrite prevention.

Run from the repository root:

```sh
Rscript tests/test_issue94_dates.R
python3 tests/test_issue94_staging.py
Rscript tests/replay_issue94_dates.R MH.RData IE.RData codes.json empty-output-directory
python3 scripts/stage_issue94_json.py original.json empty-output-directory corrected.json --source-sha256 ORIGINAL_SHA256
```

Full merge/save/round-trip checks passed for both the April/May 2025 snapshots
used in the live bundle and the current S3 July/August 2025 source snapshots.
All 40 tables retain their rows/fields and all non-target values against the
unmodified-code historical replay. All Elephant source tables remain exact.
Existing missing-column-removal warnings remain; the new unresolved-token
warning is deliberate. The full ERA–Carob rebuild retains its existing numeric
coercion warning.

## Concrete historical successor

The live May 2025 bundle was downloaded by version ID and verified against SHA256
`d63055e34680d13331c6181b985eff6a6e82056803049b95452fe2264fe9223f`.
Its JSON SHA256 is
`d5b97bfb9ae3580c721108065de3e9b8f4aaabb06f91657ea296636f890d7757`.

The local date-only successor changes **194,603 cells**: **50,704 shifted dates
corrected** and **143,899 previously lost dates recovered**. Every other value,
including exact decimal values and numeric strings, and every row position is
unchanged. Full raw re-export is not the proposed historical deployment: it also
changes six numeric-string representations and historical ordering in Soil.Out.
The source-aligned date-only staging avoids those unrelated changes.

| Field | Changed cells |
|---|---:|
| Data.Out.PD.Plant.Start | 72,945 |
| Data.Out.PD.Plant.End | 72,945 |
| Data.Out.PD.Harvest.Start | 22,251 |
| Data.Out.PD.Harvest.End | 22,243 |
| Data.Out.ED.Sample.Start | 2,439 |
| Data.Out.ED.Sample.End | 1,586 |
| Fert.Method.F.Date | 193 |
| Res.Method.M.Date | 1 |

JSON SHA256:
`c02bace9352800d7f3c6c07303753efe36b2e5dce52728dc70ae03249da1f0cb`.

## Source exceptions and holds

There are 618 unparsed source tokens: DK0117 has 584 two-digit-year planting
bounds; EO0115 has eight `30/02/2011` harvest bounds; AG0029 has 22 month-only
fertilizer dates; NJ0022 has four day/month residue dates without a year.

A separate 282 field findings retain existing typed dates: DK0117 has 256
PD.Out bounds in years 0005–0012; NN0549 has 26 year-9999 bounds across PD.Out
and Data.Out. These are preserved evidence, not validated chronology. The
handover's EO0086 unknown-year example was not reproduced in this exact pinned
cohort. No universal meaning for year 9999 was adopted. The documented time-label
sentinel 99999 is a distinct representation.

The package includes 900 row/field holds plus the complete raw-date audit, keyed
by table, merged row and field in this exact JSON. Missing dates alone must not
be interpreted as permission for climate estimation. Do not infer centuries,
calendar years or missing days on held rows without reviewed source evidence.

## Downstream verification and remaining work

The compiled comparison pipeline reads source comparison parquet, not this
agronomic JSON. Its Hippo formatter already uses the Excel origin. Bundled
comparison parquet and vocabulary are byte-identical in the proposed successor;
there is no justified broad comparison or legacy climate-statistics rerun for
this serialization correction. AG0094's 2008 comparison dates were inspected;
its 2007 planting / 2008 harvest chronology is already correct.

A private copy of ERA–Carob at `9bc4f2c511e2b9cad3afeeec75b501aa75af3ec6`
ran the complete `audit/build.R` against the date-only successor. It preserves
110,277 ERA rows, 246,993 Carob rows, source-row identity, every Carob record and
all non-date fields. Changed consumer fields are planting, raw planting,
transplanting and harvest dates. There are now 40,655 accepted ERA planting dates;
29,506 additional raw dates remain excluded by the existing same-year season
filter. Cross-year season interpretation remains unresolved. Of accepted rows,
1,237 lack coordinates, so accepted dates do not imply climate readiness.

SF-001 needs a deliberately updated input pin and snapshot expectations: the old
assertion that every ERA planting date is missing applies only to the old pin.
SF-007 remains its climate component; rerun affected joins/windows and verify
independent source-grid values after the corrected input is adopted. No reviewed
production climate artifact was available to regenerate or certify here.
SF-003 should document date origin, ISO/day precision, source holds, exact
snapshot and the restrictive season filter. Preserve the agreed one-repository,
one-planned-CGSpace-record approach.

## Deployment boundary

No production object, release pointer, workbook, viewer or adopted SR463 package
was changed. Both existing ERA sessions were consulted; dirty ERA_dev main and
ERA–Carob checkout were preserved. Code PR review and Pete's approval of the exact
publishing plan precede deployment. Issue 94 remains open until published objects
and affected consumer outputs pass post-deployment checks.

Private evidence and proposed publishing manifest:
`/Users/pstewarda/Documents/rprojects/_era-review/issue94-dates-2026-11-23/`.
