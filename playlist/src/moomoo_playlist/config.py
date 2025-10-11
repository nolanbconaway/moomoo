"""Shared configuration values for playlists."""
from uuid import UUID

# Special purpose artists are artists that are used for a special purpose, such as
# "Various Artists" for compilations. They are not real artists, so should pass thru
# max artist count logic, etc.
#
# Docs: https://musicbrainz.org/doc/Style/Unknown_and_untitled/Special_purpose_artist
SPECIAL_PURPOSE_ARTISTS = {
    UUID("f731ccc4-e22a-43af-a747-64213329e088"),  # anonymous
    UUID("33cf029c-63b0-41a0-9855-be2a3665fb3b"),  # data
    UUID("314e1c25-dde7-4e4d-b2f4-0a7b9f7c56dc"),  # dialogue
    UUID("eec63d3c-3b81-4ad4-b1e4-7c147d4d2b61"),  # no artist
    UUID("9be7f096-97ec-4615-8957-8d40b5dcbc41"),  # traditional
    UUID("125ec42a-7229-4250-afc5-e057484327fe"),  # unknown
    UUID("89ad4ac3-39f7-470e-963a-56509c546377"),  # various artists
}

# Used in exponential formula to convert the similarity score to a multiplier.
# like: exp((score - baseline) * scalar)
#
# See the script: scripts/artist_cf_score_analysis.py for more info on how these were derived.
#
# The baseline is also effectively the default value in case we have no artist similarity data,
# as the query coalesces to 1 in that case.
CF_SCALAR = 0.5
CF_BASELINE = 0.363023320190634


# i looked at the most similiar tracks and found that up until this point, the tracks were more or
# less the same (sometimes different artists, but are silent tracks, etc).
#
# This will only exclude the ~700 most similar pairs.
MINIMUM_COSINE_SIMILARITY = 0.5
