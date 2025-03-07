# Map your data in R with Nomic Atlas

This is a bare bones, experimental repo to upload straight to Nomic Atlas from R.

### Install

```R
devtools::install_github("nomic-ai/Ratlas")
```

### Get an API Key from `atlas.nomic.ai`

Add your key directorly to `~/.Renviron`

```
ATLAS_API_KEY=nk-12345ABCDER
```

Create a temporary environment variable
```R
Sys.setenv(ATLAS_API_KEY="nk-1234556")
```

### Upload data

Right now, this package doesn't support incremental data updates because we are in the process of removing
the unique id constraint from atlas. TBD.

```R
library(Ratlas)
library(tidyverse)
tibble = read_tsv("/path/to/file.tsv")

# The viewer stores your credentials; it is optional, but speeds things up.
viewer = newAtlasViewer()

dataset_name = "my-dataset"
indexed_field = "full-text"

# The flow for creating a map is building the dataset, adding data, and then requesting the creation of a map.
create_dataset(dataset_name, public=FALSE, viewer = viewer) |>
  add_dataframe(tibble, viewer = viewer) |>
  build_map(indexed_field = indexed_field, colnames = colnames(tibble), viewer=viewer)
```

Go to atlas.nomic.ai and visit your organization.
