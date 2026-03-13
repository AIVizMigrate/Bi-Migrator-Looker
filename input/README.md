# Input Directory

Place your Looker LookML files and projects here for migration testing.

## Supported Input Formats

- **Project directories**: Complete Looker projects with `.lkml` files
- **View files**: Individual `.view.lkml` files
- **Model files**: `.model.lkml` files with explore definitions

## Example Structure

```
input/
├── sample_project/
│   ├── views/
│   │   ├── orders.view.lkml
│   │   ├── customers.view.lkml
│   │   └── products.view.lkml
│   └── models/
│       └── ecommerce.model.lkml
└── single_view.view.lkml
```

## Running Migration

```bash
# Migrate a project
python -m looker_migrator.main ./input/sample_project -o ./test_output

# Migrate a single view
python -m looker_migrator.main ./input/single_view.view.lkml -o ./test_output
```
