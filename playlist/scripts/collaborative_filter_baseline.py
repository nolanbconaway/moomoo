"""Quick analysis script to determine a reasonable baseline for collaborative filtering scores."""

# %%
import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotnine as p9
import psycopg

# %%
print("loading data from database...")
sql = """
select
    artist_mbid_a
    , min(exp(score_value)) as min_score_value
    , percentile_cont(0.5) within group (order by exp(score_value)) as median_score_value
    , avg(exp(score_value)) as avg_score_value
    , max(exp(score_value)) as max_score_value
    , count(1) as num_scores

from moomoo.listenbrainz_collaborative_filtering_scores
group by artist_mbid_a
"""
with psycopg.connect("dbname=postgres") as conn:
    cur = conn.cursor()
    cur.execute(sql)
    scores = pd.DataFrame(cur.fetchall(), columns=[x[0] for x in cur.description])

# %%
agg_data = scores.describe()
print(agg_data)


# %%
baseline = np.log(agg_data.loc["50%", "median_score_value"])
print("baseline (exp space):", np.exp(baseline))
print("baseline (real space):", baseline)

# %%

lb = np.log(scores.min_score_value.min())
ub = np.log(scores.max_score_value.max())
print("Range of scores (exp space):", np.exp(lb), np.exp(ub))
print("Range of scores (real space):", lb, ub)

# get 25-75 of medians to annotate where most of the density is
p25 = np.log(agg_data.median_score_value["25%"])
p75 = np.log(agg_data.median_score_value["75%"])
print("25-75 of median scores (exp space):", np.exp(p25), np.exp(p75))
print("25-75 of median scores (real space):", p25, p75)

# %%
# plot out some options.
similarity = np.arange(lb, ub, 0.01)
scalars = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]
df = []
for s in scalars:
    df.append(
        pd.DataFrame(
            {
                "similarity": similarity,
                "scalar": s,
                "multiplier": np.exp(s * (similarity - baseline)),
            }
        )
    )
df = pd.concat(df)
title = "\n".join(
    [
        "Effect of similarity scalar on collaborative filtering score multiplier",
        f"As of {datetime.date.today().isoformat()}",
        f"Baseline score (median of medians): {baseline:.2f} (exp: {np.exp(baseline):.2f})",
    ]
)
plot = (
    p9.ggplot(
        df,
        p9.aes(x="similarity", y="multiplier", color="factor(scalar)"),
    )
    + p9.geom_line()
    + p9.geom_hline(yintercept=1, linetype="dashed")
    + p9.geom_vline(xintercept=[p25, p75], linetype="dotted", color="grey")
    + p9.scale_y_continuous(breaks=np.arange(0, 5, 0.5))
    + p9.scale_x_continuous(breaks=np.arange(-1, 1.1, 0.2))
    + p9.labs(
        title=title,
        x="collaborative filtering similarity score",
        y="playlist similarity multiplier",
        color="scalar",
    )
    + p9.theme_bw()
    + p9.theme(figure_size=(8, 5))
)

target_path = Path(__file__).parent / "cf_baseline_plot.png"
print(f"saving plot to {target_path}")
plot.save(target_path, dpi=300)
