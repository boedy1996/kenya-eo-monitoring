# Incomplete weekly TIFF sources found during production preflight

All 1,045 weekly TIFF headers were opened on the server. Three Nakuru 2025 files have incomplete band descriptions and pixel content. Band-position mapping is established by the weekly writer, but missing source pixels cannot be recovered by the index writer.

| Week | Missing input bands | Evidence from 1-in-101 sampled 256×256 blocks | Consequence |
| --- | --- | --- | --- |
| 2025-05-26 | B02, B03, B04 | Bands 1–3 have 0% finite pixels and generic `band_1`–`band_3` descriptions; B05/B08/B11 have about 45% finite pixels | NDVI, EVI, PVR and other indices requiring red/blue/green cannot be correct |
| 2025-06-02 | B11 (also B12) | Band 9 has no description and a median of zero; B02–B8A have about 16.5% finite pixels | NDMI and MSI cannot be correct |
| 2025-06-16 | B04 through B12 | Bands 3–10 have no descriptions and a median of zero; B02/B03 have about 34% finite pixels | Most indices cannot be correct |

Adjacent 2025 weekly files have all ten expected band descriptions and nonzero sampled medians. The production program will skip these three sources, record them in `/data/indices/recompute_weekly_indices_source_issues.json`, and resume them after the weekly TIFFs are repaired. It does not modify `/data/weekly/`.
