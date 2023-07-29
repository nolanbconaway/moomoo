{%- macro recording_md5(track, artist, release) -%}
{#-
    Generate a surrogate key based on the same uniqueness listenbrainz uses for msid.

    The best docs i can find on this is that listenbrainz uses a hash of the recording and artist name.
    But in practice i have found different msids for the same recording and artist name, for tracks that
    are on different albums. So i am including the album name in the hash as well.

    Docs: https://community.metabrainz.org/t/where-does-messybrainz-data-come-from/580232/2
-#}
{%- set track_= "trim(lower(" ~ track ~ "))" -%}
{%- set artist_= "trim(lower(" ~ artist ~ "))" -%}
{%- set release_= "trim(lower(" ~ release ~ "))" -%}
{{ dbt_utils.generate_surrogate_key([track_, artist_, release_]) }} 
{%- endmacro -%}