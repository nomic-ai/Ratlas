# Load required packages
library(httr)
library(jsonlite)
library(arrow)
library(readr)
library(dplyr)

# Helper function to get an access token based on an API key.
# This assumes the key is always an "nk-" series API key.
get_access_token <- function(apiKey, apiLocation = "api-atlas.nomic.ai") {
  if (is.null(apiKey) || apiKey == "") {
    stop("Could not authorize you with Nomic. Please set ATLAS_API_KEY in your environment.")
  }

  if (startsWith(apiKey, "nk-")) {
    # Use the nk- API key directly.
    return(list(token = apiKey, refresh_token = NULL, expires = as.numeric(Sys.time()) + 80))
  }

  protocol <- ifelse(startsWith(apiLocation, "localhost"), "http", "https")
  url <- paste0(protocol, "://", apiLocation, "/v1/user/token/refresh/", apiKey)

  response <- GET(url)

  if (status_code(response) >= 500 && status_code(response) < 600) {
    stop("Cannot establish a connection with Nomic services.")
  }

  if (status_code(response) != 200) {
    stop("Could not authorize you with Nomic. Run `nomic login` to re-authenticate.")
  }

  parsed <- content(response, as = "parsed", type = "application/json")
  access_token <- parsed$access_token
  if (is.null(access_token)) {
    stop("Could not authorize you with Nomic. Please check your ATLAS_API_KEY.")
  }

  return(list(token = access_token, refresh_token = apiKey, expires = as.numeric(Sys.time()) + 80))
}

# Constructor function that creates a viewer object.
# Parameters:
#   - use_env_token: logical; if TRUE, uses ATLAS_API_KEY from the environment.
#   - apiKey: explicit API key.
#   - apiLocation: API domain (default taken from ATLAS_API_DOMAIN env variable).
newAtlasViewer <- function(use_env_token = TRUE, apiKey = NULL, apiLocation = NULL) {
  # Use a more idiomatic way to fetch env variables with defaults.
  if (is.null(apiLocation) || apiLocation == "") {
    apiLocation <- Sys.getenv("ATLAS_API_DOMAIN", "api-atlas.nomic.ai")
  }

  credentials <- NULL
  if (use_env_token) {
    apiKey <- Sys.getenv("ATLAS_API_KEY", "")
    credentials <- get_access_token(apiKey, apiLocation)
  } else if (!is.null(apiKey)) {
    credentials <- get_access_token(apiKey, apiLocation)
  } else {
    stop("API key must be provided either via parameter or environment variable")
  }

  version <- "0.1.0"  # Define your version string

  # The apiCall function mimics the TypeScript method.
  apiCall <- function(endpoint, method = "GET", payload = NULL, headers = NULL,
                      options = list(octetStreamAsUint8 = FALSE)) {
    # Set default headers.
    if (is.null(headers)) {
      headers <- list(Authorization = paste("Bearer", credentials$token))
    }
    headers[["User-Agent"]] <- paste0("ts-nomic/", version)

    # Handle payload serialization.
    # If payload is an Arrow Table, serialize it as an IPC file.
    if (inherits(payload, "Table")) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("The 'arrow' package is required to serialize Arrow tables.")
      }
      buffer = arrow::BufferOutputStream$create(1000)
      write_feather(payload, buffer)
      body = buffer$finish()$data()
      headers[["Content-Type"]] <- "application/octet-stream"
    } else if (is.raw(payload)) {
      headers[["Content-Type"]] <- "application/octet-stream"
      body <- payload
    } else if (!is.null(payload)) {
      headers[["Content-Type"]] <- "application/json"
      body <- toJSON(payload, auto_unbox = TRUE)
    } else {
      headers[["Content-Type"]] <- "application/json"
      body <- NULL
    }

    protocol <- ifelse(startsWith(apiLocation, "localhost"), "http", "https")
    if (!startsWith(endpoint, "/")) {
      endpoint <- paste0("/", endpoint)
    }
    url <- paste0(protocol, "://", apiLocation, endpoint)

    headers <- vapply(headers, as.character, character(1))

    response <- VERB(method, url, body = body, add_headers(.headers = headers))

    status <- status_code(response)
    if (status < 200 || status > 299) {
      resp_text <- content(response, as = "text", encoding = "UTF-8")
      stop(sprintf("API Error %d: %s\nResponse: %s", status,
                   http_status(response)$message, resp_text))
    }

    # Deserialize the response.
    resp_content_type <- headers(response)[["content-type"]]
    if (!is.null(resp_content_type) && grepl("application/json", resp_content_type)) {
      return(content(response, as = "parsed", type = "application/json"))
    } else if (!is.null(resp_content_type) && grepl("application/octet-stream", resp_content_type)) {
      raw_data <- content(response, as = "raw")
      # Check if the first 5 bytes are the magic string "ARROW"
      if (identical(rawToChar(raw_data[1:5]), "ARROW")) {
        if (isTRUE(options$octetStreamAsUint8)) {
          return(raw_data)
        } else if (requireNamespace("arrow", quietly = TRUE)) {
          con <- rawConnection(raw_data)
          on.exit(close(con), add = TRUE)
          tbl <- arrow::read_ipc_stream(con)
          return(tbl)
        } else {
          warning("arrow package not available, returning raw data")
          return(raw_data)
        }
      } else {
        return(raw_data)
      }
    } else if (is.null(resp_content_type)) {
      return(NULL)
    } else {
      stop(sprintf("Unknown unhandled content type: %s", resp_content_type))
    }
  }

  # Return an object (as a list) that mimics the AtlasViewer.
  return(list(
    apiLocation = apiLocation,
    credentials = credentials,
    anonymous = FALSE,
    apiCall = apiCall
  ))
}

#' Create a new project
#'
#' @param dataset_name the name of the project
#' @param viewer The user access the project
#'
#' @return A list including the created dataset id
#' @export
#'
#' @examples
create_dataset = function(dataset_name, public=FALSE, viewer=NULL) {
  # Deprecated concept, still necessary for a short period
  unique_id_field = "row_number"
  # Deprecated concept, still necessary for a short period
  modality='text'
  message("Creating project")
  if (is.null(viewer)) viewer = newAtlasViewer();
  org_id = viewer$apiCall("/v1/user")$organizations[[1]]$organization_id
  options = list(
    organization_id = org_id,
    unique_id_field = unique_id_field,
    project_name = dataset_name,
    is_public = public
  )
  options[['modality']] = modality
  data = viewer$apiCall("/v1/project/create", 'POST', options)
  data
}

add_dataframe = function(project_response, dataframe, viewer=NULL) {
  message("Adding data")

  if (is.null(viewer)) viewer = newAtlasViewer()
  table = dataframe |>
    mutate(row_number = 1:n() |> as.character()) |>
    mutate(across(where(~lubridate::is.Date(.x)), ~as.POSIXct(.x))) |>
    mutate(across(where(~is.logical(.x)), ~as.character(.x))) |>
    as_arrow_table()
  # Add metadata
  table = table$cast(
    table$schema$WithMetadata(list(project_id=project_response$project_id, on_id_conflict_ignore = "true"))
  )
  viewer$apiCall(
    '/v1/project/data/add/arrow',
    'POST',
    payload=table
  )
  project_response
}

build_map = function(project_response, indexed_field, colnames, viewer=NULL) {
  message("Building map")
  response <- viewer$apiCall(
    '/v1/project/index/create',
    'POST',
    payload = list(
      project_id = project_response$project_id,
      indexed_field = indexed_field,
      colorable_fields = colnames,
      atomizer_strategies = c('document', 'charchunk'),
      geometry_strategies = list(list('document')),
      model = 'nomic-embed-text-v1.5',
      model_hyperparameters = jsonlite::toJSON(
        list(
          dataset_buffer_size = 1000,
          batch_size = 20,
          polymerize_by = 'charchunk',
          norm = 'both'
        ),
        auto_unbox = TRUE
      ),
      nearest_neighbor_index = 'HNSWIndex',
      nearest_neighbor_index_hyperparameters = jsonlite::toJSON(
        list(
          space = 'l2',
          ef_construction = 100,
          M = 16
        ),
        auto_unbox = TRUE
      ),
      projection = 'NomicProject',
      projection_hyperparameters = jsonlite::toJSON(
        list(
          n_neighbors = 15,
          n_epochs = 50,
          spread = 1
        ),
        auto_unbox = TRUE
      ),
      topic_model_hyperparameters = jsonlite::toJSON(
        list(
          build_topic_model = TRUE,
          community_description_target_field = indexed_field,
          cluster_method = 'fast',
          enforce_topic_hierarchy = FALSE
        ),
        auto_unbox = TRUE
      ),
      duplicate_detection_hyperparameters = jsonlite::toJSON(
        list(
          tag_duplicates = FALSE,
          duplicate_cutoff = 0.1
        ),
        auto_unbox = TRUE
      )
    )
  )
}
