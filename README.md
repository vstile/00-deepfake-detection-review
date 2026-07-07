## deepfake-detection-review

# Deepfake Detection – Literature Review Protocol and Data

This repository documents the end-to-end process used to scope, retrieve, and clean a corpus for a PhD literature review on **deepfake detection** with a focus on facial content. It includes the exact query sets, per-database exports, and the de-duplication workflow that produces the final screening list.

---

## 1) Scope and sources

Primary databases:

* **Scopus**
* **ScienceDirect**
* **IEEE Xplore**

Discovery only (not used for quantitative counts nor inclusion decisions):

* **Google Scholar** (useful for seeding, novelty spotting, and snowballing)

Rationale: the review prioritizes venues and indexes with stable metadata and peer review. Scholar often surfaces preprints, partial notes, or repositories. When Scholar points to a citable item, it is usually indexed in Scopus, ScienceDirect, or IEEE Xplore.

---

## 2) Query sets

Four Boolean sets were defined. Set 0 frames the domain; Sets A–C target specific method families.

```
Set 0 (domain scoping)
"deepfake detection" AND ("face" OR "faces")

Set A (physiological and geometric cues)
"deepfake detection" AND ("head pose" OR "eye blinking" OR "lip-sync" OR "facial landmarks" OR "physiological signals")

Set B (spectral domain and artifacts)
"deepfake detection" AND ("frequency domain" OR "spectral artifacts" OR "compression artifacts" OR "color filter array" OR "noise patterns")

Set C (video dynamics and graphs)
"deepfake detection" AND ("optical flow" OR "temporal consistency" OR "LSTM" OR "graph neural networks" OR "spatio-temporal")
```

---

## 3) Scoping counts

Used only to frame the size of the space, not for screening.

| Engine         |  Set 0 | Set A | Set B | Set C |
| -------------- | -----: | ----: | ----: | ----: |
| Google Scholar | 11,600 | 3,640 | 3,340 | 5,290 |

---

## 4) Targeted retrieval by database (fielded queries)

Matches constrained to article keywords and related metadata.

| Database       | Set 0 | Set A | Set B | Set C |
| -------------- | ----: | ----: | ----: | ----: |
| Scopus         |   379 |    19 |    71 |   105 |
| ScienceDirect  |    75 |    30 |    30 |    51 |
| IEEE Xplore    |   395 |    41 |   161 |   243 |
| Google Scholar | 6,670 | 2,180 | 1,900 | 3,410 |

From the screening stage onward, **Google Scholar is excluded** from the primary corpus. It remains a discovery channel.

---

## 5) De-duplication workflow

Two levels:

1. **Within each set (A, B, C)** across Scopus, ScienceDirect, IEEE Xplore
2. **Across sets** by merging the three within-set lists

### Identity keys

* Primary: **DOI**, normalized by stripping protocol prefixes and trailing punctuation, then lowercasing.
* Fallback: **Title**, normalized with Unicode NFKD, lowercased, punctuation removed, and single-space collapsed.

### Retention precedence

When duplicates refer to the same work, keep the record with the most stable metadata:

**Scopus** > **IEEE Xplore** > **ScienceDirect**

### Results

**Per set**

| Scope                        | Input | Duplicates removed | Unique |
| ---------------------------- | ----: | -----------------: | -----: |
| Query A (physio/geometry)    |    90 |                  6 |     84 |
| Query B (spectral/artifacts) |   262 |                 25 |    237 |
| Query C (spatio-temporal)    |   399 |                 76 |    323 |

**Cross-set merge (A + B + C)**

* Initial combined input: **644**
* Cross-set duplicates removed: **26**
* **Final unique records for screening: 618**

Intersections between method families:

* A ∩ B = 21
* A ∩ C = 3
* B ∩ C = 2
* A ∩ B ∩ C = 0

These small overlaps are consistent with the targeted design of Sets A–C.

---

## 6) Other sources

A small number of items reached the author through professional activities and ancillary channels (seminars, collaborations, challenge or dataset sites that link to citable papers). These are tracked as **Other** in the PRISMA-style diagram and were admitted only if they satisfied the same inclusion and exclusion criteria as database-retrieved records. Preprints are retained if they are the authoritative reference for a dataset or benchmark, or until a peer reviewed version appears.

---

## 7) Repository layout

Structure of this repo:

```
README.md
included-in-review_25.csv

/raw/
  query-0_Scopus_379.csv
  query-0_ScienceDirect_75.txt
  query-0_IEEEXplore_395.csv
  query-A_Scopus_19.csv
  query-A_ScienceDirect_30.txt
  query-A_IEEEXplore_41.csv
  query-B_Scopus_71.csv
  query-B_ScienceDirect_30.txt
  query-B_IEEEXplore_161.csv
  query-C_Scopus_105.csv
  query-C_ScienceDirect_51.txt
  query-C_IEEEXplore_243.csv

/processed/
  query-A_merged_deduplicated.csv
  query-B_merged_deduplicated.csv
  query-C_merged_deduplicated.csv
  query-ABC_merged_deduplicated.csv

/scripts/
  deduplicate.py
  parse_sciencedirect.py
  make_abc.py
```

---

## 8) Reproduction notes

Normalization helpers (Python):

```python
import re, unicodedata

def norm_doi(s):
    if not s:
        return ""
    s = s.strip().lower()
    s = s.replace("https://doi.org/","").replace("http://doi.org/","").strip().strip(".")
    m = re.search(r"(10\.\d{4,9}/\S+)", s)
    return m.group(1).rstrip(".,;)") if m else ""

def norm_title(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
```

* Primary key: `norm_doi(record) or norm_title(record)`
* Precedence for ties: Scopus > IEEE Xplore > ScienceDirect
* CSV parsing of ScienceDirect exports may need a simple text parser because some exports are TXT blocks. The repository includes `parse_sciencedirect.py` with a DOI-anchored extractor.

---

## 9) Using the outputs

* Use `processed/query-ABC_merged_deduplicated.csv` (618 records) for screening and synthesis.
* The PRISMA-style flow in the thesis references these counts. The final box in that diagram points to the evidence map table in the thesis.

---

## 10) Citation

If you use this material, please cite the thesis and this repository. Suggested citation:

GitHub repository attribution
```
Stile, V. (2025). Deepfake Detection – Literature Review, Queries, and Raw Data.
GitHub repository, https://github.com/vstile/deepfake-detection-review
© 2025 Vittorio Stile - Licensed under CC BY 4.0.
```

or journal paper attribution
``` 
This material reuses data and methods from this paper:
Stile, V., Caldelli, R., Guerrero-Contreras, G., Balderas-Díaz, S., & Medina-Bulo, I. (2026). Facial Attribute-Aware DeepFake Detection through Semi-Supervised Facial Attribute Labeling. Frontiers in Imaging, 5. https://doi.org/10.3389/fimag.2026.1846377
© 2026 Vittorio Stile - Licensed under CC BY 4.0.
```
BibTeX
```
@article{stile_facial_2026,
	title = {Facial {Attribute}-{Aware} {DeepFake} {Detection} through {Semi}-{Supervised} {Facial} {Attribute} {Labeling}},
	volume = {5},
	issn = {2813-3315},
	url = {https://www.frontiersin.org/journals/imaging/articles/10.3389/fimag.2026.1846377/full},
	doi = {10.3389/fimag.2026.1846377},
	abstract = {This study investigates the correlation between misclassifications in DeepFake detection and high-level facial attributes. A pre-trained frame-level classifier is used to distinguish manipulated from authentic video content, and its wrong predictions are analyzed in detail. To enrich the dataset, we automatically annotate each video with additional labels, including gender, hair color, hair length, ear visibility, and ethnicity, using a semi-supervised facial-attribute recognition pipeline. We extend this analysis with controlled training-time exclusions on FaceForensics++, keeping a unified test set to isolate generalization bias. Compared to the no-exclusion baseline (Accuracy = 0.806, AUC = 0.823), excluding samples with ears visible yields the largest degradation (Accuracy = 0.741, AUC = 0.763), while excluding non-visible ears has a milder effect (Accuracy = 0.813, AUC = 0.832). Hair length shows a moderate but consistent impact that interacts with ear visibility. We also explain the observed confusion-matrix asymmetry as a consequence of fixed score thresholds and video-level k-of-n aggregation. The results demonstrate that ear visibility is a critical factor for robust FAKE versus REAL discrimination and motivate attribute-aware training, including targeted data curation, attribute-specific augmentation, and threshold calibration. The proposed framework provides actionable guidance for bias-aware training strategies and supports the development of more interpretable and operationally reliable DeepFake detection systems.},
	language = {English},
	urldate = {2026-06-12},
	journal = {Frontiers in Imaging},
	publisher = {Frontiers Media SA},
	author = {Stile, Vittorio and Caldelli, Roberto and Balderas-Díaz, Sara and Guerrero-Contreras, Gabriel and Medina-Bulo, Inmaculada},
	month = may,
	year = {2026},
	keywords = {DeepFake Detection, FaceForensics++, Facial attribute analysis, Semi-supervised labeling, misclassification analysis},
}
```

or conference paper attribution
``` 
This material reuses data and methods from this paper:
Stile, V., Caldelli, R., Guerrero-Contreras, G., Balderas-Díaz, S., & Medina-Bulo, I. (2025). Analysis of DeepFake Detection through Semi-Supervised Facial Attribute Labeling. Proceedings of the 11th Spanish-German Symposium on Applied Computer Science (SGSOACS 2025), Communications in Computer and Information Science (CCIS), Applied Computer Science(1), 73–88. https://doi.org/10.1007/978-3-032-14816-2
© 2025 Springer Nature
```
BibTeX
```
@inproceedings{stile_analysis_2025,
	address = {Vienna, Austria},
	series = {Communications in {Computer} and {Information} {Science} ({CCIS})},
	title = {Analysis of {DeepFake} {Detection} through {Semi}-{Supervised} {Facial} {Attribute} {Labeling}},
	volume = {2831},
	copyright = {Creative Commons Attribution 4.0 (CC BY 4.0) International License},
	isbn = {978-3-032-14815-5},
	shorttitle = {Analysis of {DF} {Detection} through {Semi}-{Supervised} {Labeling}},
	url = {https://link.springer.com/book/9783032148155},
	abstract = {This study investigates the correlation between misclassifications in DeepFake detection and high-level facial attributes. A pre-trained frame-level classifier is used to distinguish between manipulated and authentic video contents and its wrong predictions are analyzed in detail. To enrich the dataset, we automatically annotate each video with additional labels, including gender, hair color, hair length, ear visibility and ethnicity, using a semi-supervised facial attribute recognition pipeline. An analysis of how misclassified video segments cross-reference with the visual attribute labels to identify emerging patterns is provided. Valuable insights for future bias-aware training strategies and more interpretable DeepFake detection systems are finally given.},
	language = {ENG},
	booktitle = {Proceedings of the 11th {Spanish}-{German} {Symposium} on {Applied} {Computer} {Science} ({SGSOACS} 2025)},
	publisher = {Springer Cham},
	author = {Stile, Vittorio and Caldelli, Roberto and Guerrero-Contreras, Gabriel and Balderas-Díaz, Sara and Medina-Bulo, Inmaculada},
	month = jul,
	year = {2025},
	pages = {XX, 138},
}
```

or Ph.D. thesis attribution
```
This material reuses data and methods from this Ph.D. Dissertation:
Stile, V. (2026). “AI-generated Deepfakes: Detection and Bias Analysis”. Ph.D. dissertation, Universitas Mercatorum, Roma, Italy.
© 2026 Vittorio Stile - Licensed under CC BY 4.0.
```
BibTeX
```
@phdthesis{stile_ai-generated_2026,
	address = {Rome, Italy},
	title = {{AI}-generated {Deepfakes}: {Detection} and {Bias} {Analysis}},
	copyright = {Creative Commons Attribution 4.0 (CC BY 4.0) International License, Copyright © 2026 Vittorio Stile},
	shorttitle = {{AI}-generated {Deepfakes}},
	url = {https://www.unimercatorum.iris.cineca.it/handle/20.500.12606/44725},
	abstract = {DeepFakes, synthetic manipulations of faces produced with generative artificial intelligence, threaten the authenticity of content and expose detectors to the tough task of dealing with a multiplicity of content and great variability, compression levels and deepfake generation pipelines. Against this backdrop, this doctoral thesis investigates how misclassifications in DeepFake detection relate to high-level facial attributes, and how this knowledge can guide more robust and interpretable detectors. The work proceeds in two stages. In a first analysis, a frame-level classifier distinguishes manipulated from authentic content and its errors are examined post hoc. Videos from the dataset are preprocessed by detecting and cropping faces with a cascade classifier. The dataset is enriched through a facial-attribute labeling pipeline that starts from a small manually annotated seed and expands on the whole dataset with per-attribute semi-supervised classifier to derive labels such as gender, hair color, hair length, ear visibility, and ethnicity. Subsequently, was created a DeepFake classifier that delivers achieves good results on the primary subject in each video. Attribute-wise error analysis (including label-level metrics and statistical dependence measures) reveals systematic patterns: in particular, ear visibility and hair length emerge as influential contextual factors that can a"ect decisions. In an extension of the analysis, insights are stress-tested via controlled exclusion experiments that remove one or more values of a given attribute during training, and the related models are evaluated on the complete test set. The results show that some characteristics impact model performance and decision behavior; for example, removing training exposure to certain visibility conditions degrades the detector’s ability at test time. These findings motivate data curation that balances key attribute conditions, applies targeted augmentations, and assesses the influence of attributes on the final outcome. Overall, the thesis contributes a scalable semi-supervised pipeline for attribute labeling and practical guidelines for bias-aware training. The study advances interpretability and tackles the field’s central generalization problem by showing that explicit attribute information can guide data curation and training so that models become more reliable to real-world variability.},
	language = {eng},
	urldate = {2026-07-07},
	school = {Universitas Mercatorum},
	author = {Stile, Vittorio},
	month = apr,
	year = {2026},
}
```

---

## 11) Privacy and reuse policy

* This repository contains bibliographic metadata and exported references. No personal data are included.
* **Reuse is permitted provided that you cite the author and this work.**
* Recommended license: **Creative Commons Attribution 4.0 International (CC BY 4.0)**. You are free to share and adapt the material for any purpose, even commercially, as long as appropriate credit is given, a link to the license is provided, and any changes are indicated.

---

**Availability of materials.** The complete search logs, per-database exports, deduplicated corpora for Sets A–C, the cross-set merged list, and the parsing and reconciliation scripts are publicly available in this repository.
