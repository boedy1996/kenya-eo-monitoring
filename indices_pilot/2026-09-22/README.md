# Source audit and seven-index pilot (22 September 2026)

## Scope and source lineage

- Server: `yield-forecast.geo-infinity.com`. Source folder: `/data/weekly/`. There are 1,045 weekly 10-band float32 TIFFs: 540 Nakuru and 505 Machakos, totaling 3,841,481,693,166 bytes.
- Existing TIFF tags contain no SAFE product ID or processing baseline. The current CDSE candidate inventory in `metadata_audit/2026-09-21/` shows baseline 05.xx for every weekly date, including 2016. A candidate is not proof of the exact SAFE that produced an existing TIFF.
- The retained `/opt/project/running_pipeline.log` gives stronger evidence for some weeks: it records downloads of `S2A_MSIL2A_20160118..._N0500_...SAFE`, subsequent tile processing, and creation of `nakuru_2016-01-18_2016-01-24_weekly_clean_10m.tif`. The selected 2016-01-04 pilot week was skipped as already existing in that log, so its exact source baseline remains unverified.
- The production reader divides L2A DN by 10,000 without BOA_ADD_OFFSET. For a source baseline carrying `BOA_ADD_OFFSET=-1000`, correcting an existing weekly reflectance band means subtracting 0.1 from each valid band value. This commutes with the weekly mean only if its input scenes share the same offset. The applicability is determined by processing baseline, not acquisition date.

## Pilot outputs

Five complete, seven-band TIFFs were written only to `/tmp/eo_indices_pilot/` on the server. They use float32, NaN nodata, tiled DEFLATE compression, and the same seven formulas as the current index function after subtracting 0.1 from each valid source band.

| Region | Week start | Weekly source (GB) | Pilot indices (GB) | Output/source | Time (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| Machakos | 2016-01-04 | 0.730 | 0.412 | 0.564 | 62 |
| Machakos | 2023-05-29 | 1.890 | 0.806 | 0.426 | 82 |
| Nakuru | 2016-01-04 | 3.303 | 1.673 | 0.507 | 148 |
| Nakuru | 2019-05-13 | 6.421 | 1.691 | 0.263 | 149 |
| Nakuru | 2023-05-29 | 3.533 | 0.754 | 0.213 | 102 |

Every pilot TIFF has the source grid and dimensions, seven named bands, and NaN nodata. For two Nakuru 2019 blocks, all seven bands had identical NaN masks and zero maximum difference from separately evaluated formulas. At the four Nakuru benchmark polygons, the pilot NDVI differs from the direct source-band formula by at most `1.8e-7` per pixel in the sampled 2019 and 2023 weeks. The pilot treats source value zero as nodata; 67 such band-pixels occurred across the five pilot files.

## Independent Earth Engine check

Eight Machakos fields with valid weekly pixels were checked against `COPERNICUS/S2_SR_HARMONIZED` on the same weekly dates and 10 m grid. The table uses simple means of pixel centers in each field; it is a targeted pilot, not a full historical field audit.

| Week | Matched fields | Mean corrected TIFF minus GEE | SD across fields | Largest absolute field difference |
| --- | ---: | ---: | ---: | ---: |
| 2019-07-22 | 8 | +0.0006 | 0.0020 | 0.0035 |
| 2021-07-19 | 8 | +0.0017 | 0.0240 | 0.0574 |
| 2022-01-24 | 8 | -0.0136 | 0.0089 | 0.0313 |
| 2023-05-29 | 8 | -0.0148 | 0.0151 | 0.0399 |

The 2021 outlier is MA3751, where only 36 of 66 center pixels were valid in the weekly TIFF. In the initial 2023 check, three fields had GEE data but no valid weekly pixels; a fourth had a 0.054 difference. Pixel selection and source availability therefore matter for individual fields. The selected January 2022 and May 2023 weeks have a small negative mean difference, and four sampled weeks cannot establish an era-wide shift. The earlier Nakuru comparison covers 1,176 matched field-weeks: its four per-field mean residuals range from -0.0095 to +0.0019, with pre/post mean changes no greater than 0.0047 (see `field_comparison/2026-09-21/RESIDUAL_CHECK.md`). GEE L2A does not cover the selected 2016 pilot week.

## Capacity and production recommendation

At audit time, `/data` had 1,206,072,381,440 bytes free, and `/data/indices/` contained zero files. The five temporary pilot outputs occupy about 5 GB on the root disk, outside `/data/indices/`.

Multiplying the mean observed output size per region by its weekly file count estimates **1.049 TB** for all 1,045 index TIFFs. This leaves only **0.157 TB** against current free space. Applying the largest observed output size in each region would imply **1.320 TB**, exceeding current `/data` free space. These are scenario estimates from five weeks, not a reliable upper bound.

Recommended production gate: retain `/data/weekly/` untouched; implement the correction once in a windowed index writer; record source TIFF, assumed offset/baseline policy, code version, and checksums in a manifest; generate outputs in bounded batches with size and free-space checks; compare representative fields to GEE and verify all seven bands before expanding to the full set. Stop rather than fill `/data` if the measured output sizes consume the reserve. Record that 2016-01-04 source baseline remains inferred from neighboring log evidence and the current catalogue, not directly proven.

No production code or files in `/data/indices/` were changed in this audit.
