#' @title Sectorize Comtrade to 64 MRIO sectors
#' @description Maps and aggregates product-level Comtrade data to MRIO 64-sector classification
#'
#' @param path Directory where Comtrade bulk downloads are located
#' @param report_unmapped Logical; if TRUE, generates a CSV report of unmapped HS codes
#'
#' @export

#' @import DBI
#' @import duckdb
#' @import dplyr
#' @import magrittr
#' @import glue
#' @import openxlsx
#' @import cli

sectorize_comtrade_64 <- function(path) {

  # Excel header style
  headerstyle <- openxlsx::createStyle(fgFill = "#E7E6E6", textDecoration = "bold")

  # CLI style for console messages
  cli::cli_div(theme = list(
    span.header = list(color = "cyan"),
    span.check  = list(color = "darkgreen")
  ))

  # Determine save directory
  savedir <- ifelse(file.info(path)$isdir, path, dirname(path))

  # List Comtrade files
  files <- if (file.info(path)$isdir) {
    list.files(path, pattern = "^[A-Z]{3}.*(20)[0-9]{2}.*(gz)$")
  } else {
    path
  }

  cli::cli_text("\nFound the following files:")
  for (file in files) cli::cli_bullets(c(`*` = file))
  cli::cli_text("")

  # Process each file
  for (file in files) {

    cli::cli_text(paste0("Working on ", file, "..."))

    # Connect to in-memory DuckDB
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

    # Read and filter CSV
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

    # Convert cmdCode to numeric to match concordances
    df_cleaned$cmdCode <- as.numeric(df_cleaned$cmdCode)

    # Determine HS edition
    hscode <- hscodes$edition[hscodes$code == unique(df_cleaned$classificationCode)]

    # Minor adjustment in HS->BEC mapping
    hsbecsitc$BEC5[hsbecsitc$BEC5 == 8] <- "812020"

    # Merge through the concordances
    df_sectorized <- df_cleaned %>%
      left_join(select(hsbecsitc, !!hscode, "HS12", "BEC5"), by = setNames(hscode, "cmdCode"), multiple = "any") %>%
      left_join(select(hsbec, BEC5 = "BEC5Code1", "BEC5EndUse"), multiple = "any") %>%
      left_join(hs12cpc21, multiple = "any") %>%
      left_join(cpc21isic4, multiple = "any") %>%
      left_join(isic4mrio64, multiple = "any") %>%
      mutate(
        period = as.numeric(period),
        reporterCode = as.numeric(reporterCode),
        partnerCode = as.numeric(partnerCode),
        primaryValue = as.numeric(primaryValue)
      ) %>%
      group_by(period, reporterCode, flowCode, partnerCode, BEC5EndUse, MRIO) %>%
      summarise(primaryValue = sum(primaryValue), .groups = "drop")

    # Write to Excel
    filename <- paste0(sub("\\.gz$", "", file), ".xlsx")
    sheetname <- "Sheet1"
    output <- createWorkbook()
    addWorksheet(output, sheetname)
    writeData(output, sheetname, df_sectorized, withFilter = TRUE, headerStyle = headerstyle)
    saveWorkbook(output, file.path(savedir, filename), overwrite = TRUE)

    # Disconnect
    DBI::dbDisconnect(conn, shutdown = TRUE)
    cli::cli_alert_success("Completed!")
    cli::cli_text("")
  }
}

