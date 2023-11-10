## Next: v5.0.0
**Breaking changes**
 - Changes package manage from pipenv to pdm. Docker commands starting `pipenv run ...` need to be changed to `pdm run ...`.

**Other Changes**

Engagement DB -> Analysis:
 - Adds optional `region_filter` argument to `MapConfiguration`, for controlling which regions should be drawn on a map.
 - Adds optional `legend_position` argument to `MapConfiguration`, for controlling where a map's legend should be drawn.

## v4.1.0

Engagement DB <-> Coda Sync:
- Automatically fix WS cycles (#269)
