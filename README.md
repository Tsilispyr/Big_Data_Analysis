# Big_Data_Analysis

# Big Data Management — Coursework & Research Project

Coursework, assignments, and a group research project for the graduate course **Διαχείριση Δεδομένων Μεγάλου Όγκου** ("Big Data Management"), 2025–2026. The course covers HDFS/MapReduce, Apache Spark (RDDs and Spark SQL), Neo4j, entity resolution, and similarity search, and this repository holds the assignment briefs, the code written for them, and a group research project on distributed Entity Resolution with Spark.

> This repo mixes finished, runnable code (Assignment 2, the Research Project) with assignment briefs that have no implementation here yet (Assignments 1 and 3) and a large set of lecture slides/reference PDFs. See [Status at a glance](#status-at-a-glance) below.

## Status at a glance

| # | Topic | Tooling | Deadline | Code in this repo |
|---|---|---|---|---|
| Project 1 | HDFS & MapReduce (word inverted index, frequency count, set difference) | Hadoop, Docker | 2025-11-18 | brief only ([BigData_Assign1_2025-2026.pdf](BigData_Assign1_2025-2026.pdf)) |
| Project 2 | The same 3 problems, solved with Apache Spark RDDs | Spark (Scala + Python) | 2025-12-10 |  [`Data_analysis_scala/`](Data_analysis_scala) |
| Project 3 | Parse Shakespeare into CSVs with Spark, load into Neo4j, write Cypher queries | Spark + Neo4j | see [BigData_Assign3_2025-2026.pdf](BigData_Assign3_2025-2026.pdf) | brief only |
| Research Project | Distributed Entity Resolution pipeline (blocking, similarity, matching, evaluation) | PySpark (Colab) + Scala/Spark | 2026-02-06 | [`Data_analysis_final/`](Data_analysis_final) |

## Repository structure

```
.
└── Data_analysis_final/                 # Research Project — Entity Resolution with Spark
    ├── build.sbt, project/              # sbt project (Scala 2.12.15, Spark 3.3.0)
    ├── src/main/scala/.qodo/final.scala # Local Scala/Spark port of the ER pipeline
    ├── beer_exp_data/exp_data/          # "Beer" benchmark (tableA/B, train/valid/test)
    ├── itunes_amazon_exp_data/exp_data/ # "iTunes-Amazon" benchmark (same layout)
    └── ομάδα 1/Ομάδα 1/                 # Main deliverable: Colab notebook + report
        ├── Data_analysis_final_Project.ipynb
        ├── data_analysis_final_project.py   # Notebook exported as a script
        └── Διαχείριση Δεδομένων Μεγάλου Όγκου.pdf  # full report
```

Build tool artifacts (`target/`, `.metals*/`, `.scala-build/`, `.bloop/`) are generated locally by sbt/Metals and are excluded via [.gitignore](.gitignore) — they aren't part of the source.

## Assignment 2 — Spark RDD word processing (`Data_analysis_scala/`)

Three problems, each solved two ways per the assignment spec — variant **i** loads each input file into its own RDD via `sc.textFile`, variant **ii** loads the whole `input/` directory into one RDD via `sc.wholeTextFiles`. Solutions exist in parallel Scala and Python (PySpark) versions.

Input texts (in `input/`, all Project Gutenberg / plain text): `pg100.txt` (*The Complete Works of William Shakespeare*), `pg46.txt` (*A Christmas Carol*), `el_quijote.txt` (*Don Quijote de la Mancha*, Spanish).

| Question | Task | Output shape |
|---|---|---|
| **Q1** (i / ii) | Inverted index: for every distinct word, which file(s) contain it | `word -> (pg100, el_quijote, pg46)` |
| **Q2** (i / ii) | Per-file frequency count, words of length ≥ 3 only | `word -> pg100:50, el_quijote:20, pg46:25` |
| **Q3** (i / ii) | Words that appear in `pg100.txt` but not in `pg46.txt` (set difference via `subtract`) | list of unique words |

Q1/Q2 use `union` + `reduceByKey` to merge per-file RDDs (variant i) or a single `flatMap` over `wholeTextFiles` (variant ii, which also demonstrates local in-RAM aggregation with `groupBy` in `Q2ii`). Q3 uses RDD `subtract` for set difference; `Q3ii` additionally `.cache()`s the shared `wholeTextFiles` RDD since it's filtered twice.

**Running the Scala version** (sbt, Scala 2.12.15, Spark 3.3.0 as a library dependency — no separate Spark install needed):

```bash
cd Data_analysis_scala
sbt "runMain Q1i"      # or Q1ii, Q2i, Q2ii, Q3i, Q3ii
```

Each run writes its result to `output_QXX/` as Spark part-files (an example, `output_Q1i/`, is checked in). The `build.sbt` forks the JVM and adds `--add-opens` flags because Spark 3.3's use of reflection needs them to run on modern JDKs (tested on Java 17).

**Running the Python version** (requires `pyspark` installed, e.g. `pip install pyspark`):

```bash
cd Data_analysis_scala
python Q1i.py           # or Q1ii.py, Q2i.py, Q2ii.py, Q3i.py, Q3ii.py
```

`src/main/scala/25118/` is a near-duplicate of the top-level `.scala` files (the zipped submission, named after the student ID) and `src/main/scala/copies/` holds earlier drafts of `Q2ii`/`Q3ii` — kept for reference rather than deleted.

## Research Project — Entity Resolution with Apache Spark (`Data_analysis_final/`)

Group project ("Ομάδα 1" / Team 1) implementing an end-to-end, distributed **Entity Resolution (ER)** pipeline: given two duplicate-free tables, find the record pairs that refer to the same real-world entity.

**Pipeline** (implemented with Spark SQL DataFrame functions, no UDFs, so Catalyst can optimize it):

1. **Preprocessing** — lowercase, strip punctuation via regex, remove stopwords, tokenize the concatenation of the chosen match columns.
2. **Blocking** — `explode()` each record's tokens and `join` table A against table B on shared tokens, so only records sharing at least one token become candidate pairs (avoids the full cartesian product).
3. **Entity representation** — each record is a bag-of-tokens (set of words) built from its most discriminative columns.
4. **Similarity** — Jaccard similarity on token sets via `array_intersect` / `array_union`.
5. **Matching** — pairs with `jaccard > 0.5` are predicted matches (the 0.5 cutoff is justified in the notebook via a threshold sensitivity sweep, see below).
6. **Evaluation** — ground truth is the union of each dataset's `train.csv` + `test.csv` + `valid.csv` filtered to `label == 1`; precision/recall/F1 are computed against it.

### Two parallel implementations

- **PySpark notebook (main deliverable)** — [`Data_analysis_final_Project.ipynb`](<Data_analysis_final_Project.ipynb>) (exported script: `data_analysis_final_project.py`). Built for Google Colab: it `gdown`s two datasets from the [DeepMatcher benchmark collection](https://github.com/anhaidgroup/deepmatcher/blob/master/Datasets.md) — **Fodors-Zagat** (restaurants) and **DBLP-ACM** (bibliographic citations) — then runs the pipeline above, plus:
  - A Jaccard-score histogram and a **threshold sensitivity analysis** (thresholds 0.1 → 0.9) with precision/recall/F1 plots per dataset.
  - A **hybrid similarity experiment** (average of Jaccard + normalized Levenshtein on the full concatenated string): recall rose to 97% but precision collapsed to 19%, because Levenshtein over whole records is thrown off by shared boilerplate (e.g. city/address text repeated across unrelated restaurants). The notebook documents this and reverts to plain Jaccard.
  - A written report (in Greek, embedded as markdown cells) covering design rationale and a per-dataset discussion of the sensitivity-analysis results.

  Results at threshold 0.5:

  | Dataset | Precision | Recall | F1 |
  |---|---|---|---|
  | Fodors-Zagat (restaurants) | 0.8125 | 0.9455 | 0.8739 |
  | DBLP-ACM (citations) | 0.8879 | 0.9527 | 0.9192 |

- **Scala/Spark port** — [`src/main/scala/.qodo/final.scala`](<Data_analysis_final/src/main/scala/.qodo/final.scala>) re-implements the same pipeline as a standalone `sbt` app for local execution, run against the **Beer** and **iTunes-Amazon** DeepMatcher benchmarks stored locally in `beer_exp_data/exp_data/` and `itunes_amazon_exp_data/exp_data/` (each with `tableA.csv`, `tableB.csv`, `train.csv`, `valid.csv`, `test.csv`). It hardcodes Windows paths (`D:\Data_analysis_final\...`) that will need adjusting for another machine.

**Running the notebook**: open it in Google Colab (it installs `pyspark`/`gdown` itself) or in a local Jupyter environment with `pyspark`, `pandas`, `matplotlib`, and `numpy` installed; if running locally, replace the `gdown`-based download step with local paths to the Fodors-Zagat / DBLP-ACM data.

**Running the Scala version**:

```bash
cd Data_analysis_final
# edit the folderPath values in final.scala to point at beer_exp_data/exp_data and itunes_amazon_exp_data/exp_data
sbt run
```

## Lecture materials & reference PDFs

- Root-level `lec01`–`lec08` PDFs: Intro, MapReduce/HDFS, Declarative Data Processing (MapReduce & Spark SQL), Spark, NoSQL, Entity Resolution, Similarity — duplicated under `Data_analysis_scala/Lectures/` (minus the last two).
- [`Hadoop/`](Hadoop) — HDFS/MapReduce lab guide and a Docker-based Hadoop setup walkthrough (used for Assignment 1).
- [`Spark/`](Spark) — Spark installation instructions.

## Tech stack

- **Scala** 2.12.15 on **sbt** 1.9.7, with **Apache Spark** 3.3.0 (`spark-core`, RDD API) as a library dependency — both Spark sub-projects run `local[*]` in-process, no cluster or separate Spark install required.
- **Python** + **PySpark** (DataFrame/SQL API), plus `pandas`, `numpy`, and `matplotlib` for the research project's analysis/plots, and `gdown` for dataset retrieval in Colab.
- Java 17 is supported via the `fork := true` + `--add-opens` JVM flags in both `build.sbt` files (needed because Spark 3.3's reflection-heavy internals require explicit module access on newer JDKs).

## Notes for anyone pushing this to GitHub

- A [.gitignore](.gitignore) is included to keep sbt/Metals build artifacts (`target/`, `.metals*/`, `.scala-build/`, `.bloop/`) — around 160 MB combined — out of version control.
- Large lecture/assignment PDFs and the benchmark CSV datasets are still tracked by default; if you want a leaner repo, consider Git LFS for the PDFs or excluding `Lectures/` (it duplicates the root-level slides).
- `Data_analysis_scala/output_Q1i/` is committed as a sample of Spark's output format; the other `output_Q*` folders are produced on demand by running the jobs and are not required to be tracked.
