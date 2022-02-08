## Major Changes

### Behavior changes

- `vtctl ApplySchema -uuid_list='...'` now rejects a migration if an existing migration has the same UUID but with different `migration_context`.
