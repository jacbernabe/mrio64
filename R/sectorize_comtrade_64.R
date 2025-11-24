#' @title Sectorize comtrade to 64 sectors
#' @description maps and aggregates product level trade data to mrio sectors
#'
#' @param path, replace with directory where Comtrade bulk downloads are located
#'
#' @export

# File: R/sectorize_comtrade_64.R
# Author: ADB MRIO
# Purpose: Map Comtrade HS data to MRIO 64-sector classification

sectorize_comtrade_64 <- function(path) {

  # Style for Excel headers
  headerstyle <- openxlsx::createStyle(fgFill = "#E7E6E6", textDecoration = "bold")

  # Fancy console messages
  cli::cli_div(theme = list(
    span.header = list(color = "cyan"),
    span.check  = list(color = "darkgreen")
  ))

  # Determine savedir
  savedir <- ifelse(file.info(path)$isdir, path, dirname(path))

  # List Comtrade files
  if (file.info(path)$isdir) {
    files <- list.files(path, pattern = "^[A-Z]{3}.*(20)[0-9]{2}.*(gz)$")
  } else {
    files <- path
  }

  cli::cli_text("")
  cli::cli_text("Found the following files:")
  for (file in files) cli::cli_bullets(c(`*` = paste0("{.header ", file, "}")))
  cli::cli_text("")

  # Process each file
  for (file in files) {

    cli::cli_text(paste0("Working on {.header ", file, "}..."))

    # Connect to in-memory DuckDB
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

    # Clean CSV with DuckDB SQL
    df_cleaned <- DBI::dbGetQuery(conn, glue::glue("
      SELECT period,
             reporterCode,
             flowCode,
             partnerCode,
             classificationCode,
             cmdCode,
             primaryValue
      FROM read_csv_auto('{file.path(savedir, file)}', all_varchar=1)
      WHERE partnerCode <> 0
        AND partner2Code = 0
        AND (flowCode = 'X' OR flowCode = 'M')
        AND length(cmdCode) = 6
        AND motCode = 0
        AND customsCode = 'C00'
    "))

    # Determine HS edition
    hscode <- hscodes$edition[hscodes$code == unique(df_cleaned$classificationCode)]

    # Minor adjustment in HS→BEC5 mapping
    hsbecsitc$BEC5[hsbecsitc$BEC5 == 8] <- "812020"

    # Merge through the concordances and aggregate
    df_sectorized <- suppressMessages(
      dplyr::summarise(
        dplyr::mutate(
          dplyr::left_join(
            dplyr::left_join(
              dplyr::left_join(
                dplyr::left_join(
                  dplyr::left_join(
                    df_cleaned,
                    dplyr::select(hsbecsitc, {{ hscode }}, .data$HS12, .data$BEC5),
                    by = c(cmdCode = hscode), multiple = "any"
                  ),
                  dplyr::select(hsbec, BEC5 = .data$BEC5Code1, .data$BEC5EndUse),
                  multiple = "any"
                ),
                hs12cpc21, multiple = "any"
              ),
              cpc21isic4, multiple = "any"
            ),
            isic4mrio64, multiple = "any"
          ),
          period = as.numeric(period),
          reporterCode = as.numeric(reporterCode),
          partnerCode  = as.numeric(partnerCode),
          primaryValue = as.numeric(primaryValue)
        ),
        primaryValue = sum(.data$primaryValue),
        .by = c(.data$period, .data$reporterCode, .data$flowCode,
                .data$partnerCode, .data$BEC5EndUse, .data$MRIO)
      )
    )

    # Write to Excel
    filename <- paste0(sub("\\.gz$", "", file), ".xlsx")
    sheetname <- "Sheet1"
    output <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(output, sheetname)
    openxlsx::writeData(output, sheetname, df_sectorized,
                        withFilter = TRUE, headerStyle = headerstyle)
    openxlsx::saveWorkbook(output, file.path(savedir, filename), overwrite = TRUE)

    # Disconnect
    DBI::dbDisconnect(conn, shutdown = TRUE)
    cli::cli_alert_success("{.check Completed.}")
    cli::cli_text("")
  }
}
