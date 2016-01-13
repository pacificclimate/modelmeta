library(ncdf4)
library(ncdf4.helpers)
library(RPostgreSQL)
library(RSQLite)
library(PCICt)
library(digest)
library(rgdal)
library(snow)

##file.list.rcm <- list.files("/home/data/climate/RCM/", "\\.nc$", recursive=TRUE, full.names=TRUE)

get.chunks.for.list <- function(l, chunk.size) {
  chunk.boundaries <- pmin(chunk.size * (0:(ceiling(length(l) / chunk.size))) + 1, length(l))
  chunk.starts <- chunk.boundaries[1:(length(chunk.boundaries) - 1)]
  chunk.ends <- chunk.boundaries[2:length(chunk.boundaries)] - 1
  return(lapply(1:length(chunk.starts), function(x) { l[chunk.starts[x]:chunk.ends[x]] }))
}

## This will end up being a two-step process. First, one must populate the database.
## Then, one can figure out all the interdependencies (derived_from, initialized_from, anomaly_from, etc).
index.netcdf.files <- function(file.list, host="dbhost", db="dbname", user="dbuser", password=NULL) {
  drv <- dbDriver("PostgreSQL")
  con <- NULL
  if(is.null(password))
    con <- dbConnect(drv, host=host, dbname=db, user=user)
  else
    con <- dbConnect(drv, host=host, dbname=db, user=user, password=password)

  lapply(file.list, index.netcdf, con)

  dbDisconnect(con)
}

index.netcdf.files.sqlite <- function(file.list, db.file) {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname=db.file)
  lapply(file.list, index.netcdf, con)
  dbDisconnect(con)
}

index.netcdf <- function(filename, con) {
  print(filename)
  filename <- gsub("[/]+", "/", filename)
  f <- nc_open(filename)
  dbBegin(con)
  #dbGetQuery(con, "BEGIN TRANSACTION")
  data.file.id <- get.data.file.id(f, filename, con)
  dbCommit(con)
  ##dbSendQuery(con, "ROLLBACK;")
  nc_close(f)
  return(data.file.id)
}

## Quote booleans according to the db driver
dbQuote <- function(con, s) {
  if (class(con) == 'SQLiteConnection') {
    if (s) {
      1
    } else
      0
  }
  else {
    s
  }
}

dbQuoteNow <- function(con) {
  if (class(con) == 'SQLiteConnection') {
    "datetime('now')"
  } else {
    "now()"
  }
}

## PostgreSQL has a RETURNING clause to INSERT statements while SQLite has
## the last_insert_rowid() function. Use the appropriate method and return
## the id of the inserted row
do.insert <-function(con, query, id.column.name) {
    if (class(con) == 'SQLiteConnection') {
      result <- dbSendQuery(con, query)
      result <- dbSendQuery(con, 'SELECT last_insert_rowid()')
      fetch(result, -1)
    } else {
      query <- paste(query, 'RETURNING', id.column.name)
      result <- dbSendQuery(con, query)
      ## FIXME: Should check result
      fetch(result, -1)
    }
}

## Get range of variable, one time field at a time
## But do it in parallel because we need parallel readers due to compression.
get.variable.range <- function(f, var, max.vals.per.node.millions=20) {
  num.nodes <- 8
  axes.map <- nc.get.dim.axes(f, var)
  axes.map <- axes.map[!is.na(axes.map)]
  dims.len <- f$var[[var]]$varsize
  #cluster <- makeCluster(num.nodes, "SOCK")
  #clusterEvalQ(cluster, source("/home/data/projects/model_metadata_db/index_netcdf.r"))
  #clusterEvalQ(cluster, library(ncdf4.helpers))
  node.chunks.lists <- get.chunks.for.list(1:(dims.len[axes.map == "T"]),
                                           ceiling(dims.len[axes.map == "T"] / num.nodes))
  num.timesteps.per.node <- floor((max.vals.per.node.millions * 1000000) / prod(dims.len[1:2]))
  res <- unlist(node.chunks.lists, function(idx.list) {
   ncfile <- nc_open(f$filename)
    res <- sapply(get.chunks.for.list(idx.list, num.timesteps.per.node), function(x) {
      gc()
      d <- nc.get.var.subset.by.axes(ncfile, var, list(T=x), axes.map)
      r <- range(d[!is.nan(d)], na.rm=TRUE)
      if(all(r == c(Inf, -Inf)))
        return(c(NA, NA))
      else
        return(r)
    })
    nc_close(ncfile)
    return(res)
  })

  ## res <- range(parSapply(cluster, get.chunks.for.list(1:(dims.len[axes.map == "T"]), 100), function(x) {
  ##   d <- nc.get.var.subset.by.axes(f, var, list(T=x), axes.map)
  ##   range(d, na.rm=TRUE)
  ## }))

  #stopCluster(cluster)
  return(range(res, na.rm=TRUE))
}

## Creates entries for data_file_variables
create.data.file.variables <- function(f, data.file.id, con) {
  var.list <- nc.get.variable.list(f)
  if(length(var.list) == 0) return(NULL)
  dfv.list <- lapply(var.list, function(v) {
    query <- paste("SELECT data_file_variable_id from data_file_variables where data_file_id=",
                   data.file.id, " and netcdf_variable_name='", v, "';", sep="")
    result <- dbSendQuery(con, query)
    data.file.variable.id <- fetch(result, -1)
  
    if(nrow(data.file.variable.id) == 0) {
      var.data <- f$var[[v]]
      variable.stdname.att <- ncatt_get(f, v, "standard_name")
      variable.longname.att <- ncatt_get(f, v, "long_name")
      var.stdname <- ifelse(variable.stdname.att$hasatt, variable.stdname.att$value, v)
      var.longname <- ifelse(variable.longname.att$hasatt, variable.longname.att$value, v)
      variable.units <- var.data$units
      variable.id <- get.variable.alias.id(var.stdname, var.longname, variable.units, con)
      
      variable.range <- get.variable.range(f, v)
      
      grid.id <- get.grid.id(f, v, con)
      if(is.na(grid.id))
        grid.id <- "NULL"
      
      cell.methods.att <- ncatt_get(f, v, "cell_methods")
      variable.cell.methods <- cell.methods.att$value
      if(!cell.methods.att$hasatt)
        variable.cell.methods <- NULL
      
      level.set.id <- get.level.set.id(f, v, con)
      return(paste("(", paste(data.file.id,
                              variable.id,
                              ifelse(is.null(variable.cell.methods),
                                     "NULL", shQuote(variable.cell.methods)),
                              ifelse(is.null(level.set.id),
                                     "NULL", level.set.id),
                              grid.id,
                              shQuote(v),
                              variable.range[1],
                              variable.range[2],
                              sep=",")
                   , ")", sep="")
             )
    }
  })
  
  query <- paste("INSERT INTO data_file_variables(data_file_id, variable_alias_id, variable_cell_methods, level_set_id, grid_id, netcdf_variable_name, range_min, range_max) VALUES", do.call(paste, c(dfv.list, sep=",")), ";", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  ## Should check result
}

## FIXME: Why do we strsplit after we pull the attribute out of the netcdf?
## Just use the whole thing
get.inst.name <- function(f) {
  institution <- ncatt_get(f, 0, "institution")
  return(ifelse(institution$hasatt, strsplit(institution$value, " ")[[1]][1], ""))
}

## FIXME: Same as above re: strsplit
get.project <- function(f) {
  project <- ncatt_get(f, 0, "project_id")
  return(ifelse(project$hasatt, strsplit(project$value, " ")[[1]][1], ""))
}

get.file.metadata.cmip5 <- function(f) {
  split.path <- strsplit(f$filename, "/")[[1]]
  fn.split <- strsplit(tail(split.path, n=1), "_")[[1]]

  if(length(fn.split) != 6)
    return(NA)

  names(fn.split) <- c("var", "tres", "model", "emissions", "run", "trange", rep(NA, max(0, length(fn.split) - 6)))
  fn.split[length(fn.split)] <- strsplit(fn.split[length(fn.split)], "\\.")[[1]][1]
  fn.split[c('tstart', 'tend')] <- strsplit(fn.split['trange'], "-")[[1]]
  fn.split["institution"] <- get.inst.name(f)
  fn.split["project"] <- get.project(f)
  
  fn.split
}

get.file.metadata.cmip3 <- function(f) {
  ts <- nc.get.time.series(f)
  time.res <- get.time.resolution(f, ts)

  time.fmt.cmip5.map <- c(mon="%Y%m", day="%Y%m%d")
  time.res.cmip5.map <- c(monthly="mon", daily="day")
  
  time.res.cmip5 <- time.res.cmip5.map[time.res]
  if(length(nc.get.climatology.bounds.var.list(f)) > 0)
    time.res.cmip5 <- paste(time.res.cmip5, "Clim", sep="")
  date.range.split <- format(range(ts), format=time.fmt.cmip5.map[time.res.cmip5])
  date.range <- paste(date.range.split, collapse="-")
  
  inst.name <- get.inst.name(f)
  
  source.att <- ncatt_get(f, 0, "source")
  run.att <- ncatt_get(f, 0, "run")
  model.short.name <- ifelse(source.att$hasatt,
                             strsplit(source.att$value, "[, \\(\\)\\{\\}:;]")[[1]][1],
                             ifelse(run.att$hasatt,
                                    paste(strsplit(run.att$value, " ")[[1]][2:3], sep="_"),
                                    ''
                                    )
                             )

  ghg.scenario.att <- ncatt_get(f, 0, "ghg_scenario")
  experiment.id.att <- ncatt_get(f, 0, "experiment_id")
  expt <- if(ghg.scenario.att$hasatt) ghg.scenario.att else experiment.id.att

  expt.name.map <- c("(SRES B1)"="sresb1", "(SRES A2)"="sresa2",
                     "(SRES A1B)"="sresa1b", "(20C3M)"="20c3m",
                     "IPCC SRES B1"="sresb1", "IPCC SRES A2"="sresa2",
                     "IPCC SRES A1B"="sresa1b", "IPCC 20C3M"="20c3m")
  expt.name <- "OTHER"
  if(expt$hasatt) {
    regexp.match <- regexpr("\\((.*)\\)", expt$value)
    if(regexp.match != -1)
      expt.name <- expt.name.map[substr(expt$value, regexp.match, regexp.match + attr(regexp.match, "match.length"))]
    else
      expt.name <- expt.name.map[expt$value]
  }
  
  v.list <- nc.get.variable.list(f)
  var.name <- paste(v.list, sep="-")

  run.name <- NULL
  realization.att <- ncatt_get(f, 0, "realization")
  run.att <- ncatt_get(f, 0, "run")
  source.att <- ncatt_get(f, 0, "source")
  run.name <- ifelse(realization.att$hasatt,
                     paste("run", realization.att$value),
                     ifelse(run.att$hasatt,
                            strsplit(run.att$value, "[, \\(\\)\\{\\}:;]")[[1]][1],
                            source.att$value
                            )
                     )
  stopifnot(!is.null(run.name))

  project <- get.project(f)

  return(c(var=var.name,
           tres=time.res.cmip5,
           model=model.short.name,
           emissions=expt.name,
           run=run.name,
           trange=date.range,
           tstart=date.range[1],
           tend=date.range[2],
           institution=inst.name,
           project=project))
}

get.file.metadata <- function(f) {
  meta <- get.file.metadata.cmip5(f)
  if(any(is.na(meta)))
    meta <- get.file.metadata.cmip3(f)

  return(meta)
}

create.unique.id <- function(f, filename) {
  meta <- get.file.metadata(f)
  axes <- paste("dim", paste(nc.get.dim.axes(f, nc.get.variable.list(f, min.dims=2)[1]), collapse=""), sep="")
  unique.id <- NULL
  if(axes %in% c("dimXYT", "dimXYZT")) {
    unique.id <- paste(meta[c("var", "tres", "model", "emissions", "run", "trange")], collapse="_")
  } else {
    unique.id <- paste(c(meta[c("var", "tres", "model", "emissions", "run", "trange")], axes), collapse="_")
  }
  return(gsub("[+]", "-", unique.id))
}

create.data.file.id <- function(f, filename, con) {
  first.MiB.md5sum <- get.first.MiB.md5sum(filename)
  var.list <- nc.get.variable.list(f)
  time.set.id <- get.time.set.id(f, con)
  run.id <- get.run.id(f, con)
  unique.id <- create.unique.id(f, filename)
  print(unique.id)
  
  dim.axes <- nc.get.dim.axes(f)
  dim.axes <- dim.axes[!is.na(dim.axes)]
  x.dim.name <- ifelse(any(dim.axes == "X"), shQuote(names(dim.axes)[dim.axes == "X"]), "NULL")
  y.dim.name <- ifelse(any(dim.axes == "Y"), shQuote(names(dim.axes)[dim.axes == "Y"]), "NULL")
  z.dim.name <- ifelse(any(dim.axes == "Z"), shQuote(names(dim.axes)[dim.axes == "Z"]), "NULL")
  t.dim.name <- ifelse(any(dim.axes == "T"), shQuote(names(dim.axes)[dim.axes == "T"]), "NULL")
  
  query <- paste("INSERT INTO data_files",
                 "(filename, run_id, first_1mib_md5sum, ",
                 "unique_id, time_set_id, x_dim_name, ",
                 "y_dim_name, z_dim_name, t_dim_name, index_time) ",
                 "VALUES(",
                 paste(shQuote(filename),
                       run.id,
                       shQuote(first.MiB.md5sum),
                       shQuote(unique.id),
                       time.set.id,
                       x.dim.name,
                       y.dim.name,
                       z.dim.name,
                       t.dim.name,
                       dbQuoteNow(con), sep=","),
                 ")", sep="")
  return(do.insert(con, query, 'data_file_id'))
}

update.data.file.id <- function(f, data.file.id, filename, con) {
  result <- dbSendQuery(con, paste("DELETE FROM ensemble_data_file_variables",
                                   "WHERE data_file_variable_id IN (",
                                     "SELECT data_file_variable_id",
                                     "FROM data_file_variables",
                                     "WHERE data_file_id=", data.file.id,
                                   ")"))
  result <- dbSendQuery(con, paste("DELETE FROM data_file_variables",
                                   "WHERE data_file_id=", data.file.id, ";"))
  result <- dbSendQuery(con, paste("DELETE FROM data_files",
                                   "WHERE data_file_id=", data.file.id, ";"))
  new.data.file.id <- create.data.file.id(f, filename, con)
  create.data.file.variables(f, new.data.file.id, con)
  return(new.data.file.id)
}

update.data.file.index.time <- function(f, data.file.id, con) {
  return(dbSendQuery(con, paste("UPDATE data_files",
                                "SET index_time =", dbQuoteNow(con),
                                "WHERE data_file_id =", data.file.id)))
}

update.data.file.filename <- function(f, data.file.id, new.filename, con) {
  return(dbSendQuery(con, paste("UPDATE data_files",
                                "SET index_time =", dbQuoteNow(con),
                                "filename =", shQuote(new.filename),
                                "WHERE data_file_id =", data.file.id)))
}

## Creates and returns the id of the created data file entry.
get.data.file.id <- function(f, filename, con) {
  first.MiB.md5sum <- get.first.MiB.md5sum(filename)
  unique.id <- create.unique.id(f, filename)
  query <- paste("SELECT data_file_id,filename,first_1mib_md5sum,unique_id,index_time ",
                 "FROM data_files ",
                 "WHERE first_1mib_md5sum='", first.MiB.md5sum,"' ",
                 "OR unique_id='", unique.id, "';", sep="")
  result <- dbSendQuery(con, query)
  data.file.data <- fetch(result, -1)
  
  if(nrow(data.file.data) == 0) {
    ## File does not exist in database
    data.file.id <- create.data.file.id(f, filename, con)
    create.data.file.variables(f, data.file.id, con)
    return(data.file.id)
  } else {
    ## File already in database
    stopifnot(nrow(data.file.data) == 1)

    cur.file.info <- file.info(filename)
    if(filename == data.file.data$filename) {
      if(first.MiB.md5sum == data.file.data$first_1mib_md5sum) {
        if(data.file.data$index_time > cur.file.info$mtime) {
          ## Update the index time.
          update.data.file.index.time(f, data.file.data$data_file_id, con)
        } else {
          ## File has changed w/o hash being updated; scream and yell, then reindex and update existing records.
          cat(paste("File", filename, ": Hash didn't change, but file was updated.\n"))
          return(update.data.file.id(f, data.file.data$data_file_id, filename, con))
        }
      } else {
        if(data.file.data$index_time > cur.file.info$mtime) {
          ## Error condition. Should never happen.
          cat(paste("File", filename, ": Hash changed, but mod time doesn't reflect update.\n"))
          stopifnot(FALSE)
        } else {
          ## File has changed; re-index, and update existing records.
          return(update.data.file.id(f, data.file.data$data_file_id, filename, con))
        }
      }
    } else {
      if(file.exists(data.file.data$filename)) {
        if(normalizePath(data.file.data$filename) == normalizePath(filename)) {
          ## Same file (probably a symlink). Ignore the file; we'll hit it later.
          ## FIXME: CHECK THE ASSUMPTION HERE.
          cat(paste(data.file.data$filename, "refers to the same file as", filename, "\n"))
        } else {
          ## Name changed and data changed.
          cur.file.md5sum <- digest(filename, file=TRUE)
          db.file.md5sum <- digest(data.file.data$filename, file=TRUE)
          if(cur.file.md5sum == db.file.md5sum) {
            ## Same content. Scream about a copy.
            cat(paste("File", filename, "is a copy of", data.file.data$filename, ". Figure out why.\n"))
          } else {
            ## Different content. May be a newer version of the same file...
            ## Action: Index it.
            data.file.id <- update.data.file.id(f, data.file.data$data_file_id, filename, con)
            return(data.file.id)
          }
        }
      } else {
        ## Old file does not exist. MD5sum must match.
        if(data.file.data$index_time > cur.file.info$mtime) {
          ## Probably just a move. Don't re-index it, just update the location.
          update.data.file.filename(f, data.file.data$data_file_id, filename, con)
        } else {
          ## May be more than a copy. Reindex it.
          return(update.data.file.id(f, data.file.data$data_file_id, filename, con))
        }
      }
    }
    return(data.file.data$data_file_id)
  }
}

## Gets the md5sum of the first MiB of the netcdf file
get.first.MiB.md5sum <- function(filename) {
  ## FIXME: Change digest to SHA-1 or better; md5 isn't so great.
  return(digest(readBin(filename, what=raw(), n=1024*1024, size=1), algo="md5", serialize=FALSE))
}

## Finds and returns the id of the data_file_variable which was used
## to compute the anomaly field if field is determined to be an anomaly
## field; otherwise returns NULL.
get.anomaly.from <- function(f) {
  return(NULL)
  ## STUB
}

get.time.set.id <- function(f, con) {
  time.series <- nc.get.time.series(f, return.bounds=TRUE)

  if(length(time.series) == 1 && is.na(time.series))
    return(NULL)

  multi.year.mean <- is.multi.year.mean(f)
  start.date <- format(min(time.series, na.rm=TRUE), "%Y-%m-%d %H:%M:%S")
  end.date <- format(max(time.series, na.rm=TRUE), "%Y-%m-%d %H:%M:%S")
  time.resolution <- get.time.resolution(f, time.series)
  cal <- attr(time.series, "cal")

  ## Remap time series in 360-day case.
  dpy <- attr(time.series, "dpy")
  if(!is.null(dpy) && dpy == 360) {
    mdays <- c(31, 28, 31, 30, 30, 30, 30, 30, 30, 30, 30, 30)
    mday.start <- cumsum(c(0, mdays[1:11]))
    
    years <- as.numeric(format(time.series, "%Y"))
    jdays <- as.numeric(format(time.series, "%j"))
    mons <- sapply(jdays, function(x) { sum(mday.start < x) })
    days <- jdays - mday.start[mons]
    time.series <- as.PCICt(paste(years, mons, days, sep="-"), cal="365")
  }
  
  query <- paste("SELECT time_set_id",
                 "FROM time_sets ",
                 "WHERE multi_year_mean =", dbQuote(con, multi.year.mean),
                 "AND start_date=", shQuote(start.date),
                 "AND end_date=", shQuote(end.date),
                 "AND time_resolution=", shQuote(time.resolution),
                 "AND num_times=", length(time.series),
                 "AND calendar=", shQuote(cal))
  ##print(query)
  result <- dbSendQuery(con, query)
  time.set.id <- fetch(result, -1)
  
  if(nrow(time.set.id) == 0) {
    query <- paste("INSERT INTO time_sets(",
                     "calendar,",
                     "start_date,",
                     "end_date,",
                     "time_resolution,",
                     "multi_year_mean,",
                     "num_times)",
                   "VALUES(", paste(shQuote(cal),
                                    shQuote(start.date),
                                    shQuote(end.date),
                                    shQuote(time.resolution),
                                    dbQuote(con, multi.year.mean),
                                    length(time.series),
                                    sep=","),
                   ")")
    time.set.id.stuff <- do.insert(con, query, 'time_set_id')

    dbClearResult(result)
    time.set.id <- time.set.id.stuff[1,1]

    time.bits <- paste(shQuote(format(time.series, "%Y-%m-%d %H:%M:%S")),
                       seq_along(time.series) - 1,
                       time.set.id,
                       collapse="),(",
                       sep=",")
    query <- paste("INSERT INTO times(timestep, time_idx, time_set_id) ",
                   "VALUES(", time.bits, ")" )
    ##print(query)
    result.time.bits <- dbSendQuery(con, query)
    ## FIXME: Should check result

    if(!is.null(attr(time.series, "climatology.bounds"))) {
      clim.bnds <- attr(time.series, "climatology.bounds")
      browser()
      clim.bnds.list <- lapply(1:dim(clim.bnds)[1], function(x) { return(c(clim.bnds[x,], x)) })
      time.origin.att <- ncatt_get(f, "time", "units")
      query <- paste("INSERT INTO climatological_times(time_start, time_end, time_idx, time_set_id) ",
                     "VALUES(", do.call(paste, c(lapply(clim.bnds.list,
                                                        function(x) {
                                                          paste(x[1], x[2], x[3], time.set.id, sep=",")
                                                        }),
                                                 sep="),(")), ")", sep="")
      ##print(query)
      result <- dbSendQuery(con, query)
    }
    
    return(time.set.id)
  } else {
    return(time.set.id[1,1])
  }
}

## Finds and returns a level_set_id if it can find a match and if the variable has a levels dimension.
## If no match is found for the levels, creates a new level_set.
## If there is no levels dimension, returns NULL.
get.level.set.id <- function(f, v, con) {
  dim.axes <- nc.get.dim.axes(f, v)
  levels.idx <- NULL

  if(length(dim.axes) == 4) {
    if(all(is.null(dim.axes))) {
      levels.idx <- 3
    } else {
      z.dim <- dim.axes == "Z"
      if(any(z.dim)) {
        levels.idx <- which(z.dim)
      }
    }
  }

  if(is.null(levels.idx))
    return(NULL)

  levels.dim <- f$var[[v]]$dim[[levels.idx]]
  
  levels <- levels.dim$vals

  query <- paste("SELECT level_set_id ",
                 "FROM levels NATURAL JOIN level_sets ",
                 "WHERE pressure_level IN (",
                   paste(levels, collapse=",", sep=","),
                 ") AND level_units = '", levels.dim$units, "' ",
                 "GROUP BY level_set_id ",
                 "HAVING count(vertical_level)=", length(levels), ";", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  level.set.id <- fetch(result, -1)

  if(nrow(level.set.id) == 0) {
    query <- paste("INSERT INTO level_sets(level_units) ",
                   "VALUES('", levels.dim$units, "') ", sep="")
    level.set.id.stuff <- do.insert(con, query, 'level_set_id')

    level.set.id <- level.set.id.stuff[1,1]

    levels.bnds <- get.bnds.center.array(f, levels.dim$name)
    level.bits <- do.call(paste, c(lapply(seq_along(levels),
                                          function(x) {
                                            return(paste(levels.bnds[x,1],
                                                         levels.bnds[x,2],
                                                         levels.bnds[x,3],
                                                         x - 1, level.set.id, sep=","))
                                          }),
                                   sep="),("))
    query <- paste("INSERT INTO levels(level_start, vertical_level, level_end, level_idx, level_set_id) ",
                   "VALUES(", level.bits, ")" )
    ##print(query)
    result.level.bits <- dbSendQuery(con, query)
    ## Should check result

    return(level.set.id)
  } else {
    return(level.set.id[1,1])
  }
}

## Finds and returns a variable_id if it can find a match; otherwise creates an entry and returns the id.
## Note: Changed method to consider the long name canonical
get.variable.alias.id <- function(variable.standard.name, variable.long.name, variable.units, con) {
  query <- paste("SELECT variable_alias_id ",
                 "FROM variable_aliases WHERE ",
                 "variable_long_name='", variable.long.name, "' ",
                 "AND variable_standard_name='", variable.standard.name, "' ",
                 "AND variable_units='", variable.units, "';", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  variable.id <- fetch(result, -1)
  
  if(nrow(variable.id) == 0) {
    query <- paste("INSERT INTO variable_aliases(variable_long_name, variable_standard_name, variable_units) ",
                   "VALUES(", paste(shQuote(variable.long.name),
                                    shQuote(variable.standard.name),
                                    shQuote(variable.units), sep=","),
                   ")", sep="")
    variable.id <- do.insert(con, query, 'variable_alias_id')

    return(variable.id[1,1])
  } else {
    return(variable.id[1,1])
  }
}

## Returns the spatial reference ID of the data set, or WGS84 (4326) if nothing found
get.srid <- function(f, v, con) {
  proj4.string <- nc.get.proj4.string(f, v)
  if(is.null(proj4.string) || proj4.string == "")
    proj4.string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  wkt.string <- showWKT(proj4.string)
  
  ## Need to query SRID table for proj4 string or for WKT string.
  query <- paste("SELECT srid FROM spatial_ref_sys WHERE srtext='", wkt.string, "';", sep="")
  result <- dbSendQuery(con, query)
  sr.id <- fetch(result, -1)
  
  if(nrow(sr.id) == 0) {
    ## If it doesn't exist, add some bogus looking number to the table and return that.
    
    ## Get max SRID, take max of that and 10e6
    query <- "SELECT MAX(srid) FROM spatial_ref_sys;"
    result <- dbSendQuery(con, query)
    max.srid <- fetch(result, -1)
    
    ## FIXME: This will cause concurrency problems if we ever get there.
    cur.srid <- max(max.srid[1,1], 990000) + 1
    query <- paste("INSERT INTO spatial_ref_sys(srid, auth_name, auth_srid, srtext, proj4text) ",
                   "VALUES(", paste(cur.srid, "\'PCIC\'", cur.srid,
                                    paste("'", wkt.string, "','", proj4.string, "'", sep=""),
                                    sep=","),
                   ")", sep="")
    ##print(query)
    result <- dbSendQuery(con, query)
    
    return(cur.srid)
  } else {
    ## If it does exist, return it.
    return(sr.id[1,1])
  }
}

## Gets cell average area in sq km
get.cell.avg.area <- function(x.dim, y.dim, proj4text) {
  if(x.dim$units == "m" & y.dim$units == "m") {
    ## Assume that it's regular if specified in meters
    return(abs((x.dim$vals[1] - x.dim$vals[2]) * (y.dim$vals[1] - y.dim$vals[2])) / 10e6)
  } else {
    ## Assume lat-lon for now.
    earth.radius <- 6371
    x.diff <- x.dim$vals[1] - x.dim$vals[2]
    d2r <- pi / 180
    return(mean(sapply(1:(length(y.dim) - 1), function(i) {
      y.diff <- y.dim$vals[i] - y.dim$vals[i + 1]
      return(y.diff * cos(y.dim$vals[i] * d2r) * x.diff * d2r^2 * earth.radius^2)
    })))
  }
}

## Returns an n columns by 3 rows array containing the bounds and center values for dim d
get.bnds.center.array <- function(f, d, bnds.var=NULL) {
  ## Get bounds attribute for dimension
  if(is.null(bnds.var)) {
    bnds.att <- ncatt_get(f, d$name, "bounds")
    bnds.var <- ifelse(bnds.att$hasatt, bnds.att$value, NULL)
  }
  
  if(!is.null(bnds.var)) {
    ## If it exists, get values for it
    bnds.ncvar <- ncvar_get(f, bnds.var)
    return(rbind(bnds.ncvar[1,], d$vals, bnds.ncvar[2,]))
  } else {
    ## If not, infer values
    steps <- d$vals[2:length(d$vals)] - d$vals[1:(length(d$vals) - 1)]
    half.steps.filled <- steps[c(1:floor(length(steps) / 2),
                                 ceiling(length(steps) / 2 + 0.001),
                                 ceiling(length(steps) / 2 + 0.001):length(steps))] / 2
    return(rbind(d$vals - half.steps.filled,
                 d$vals, d$vals + half.steps.filled))
  }
}

## Finds and returns a grid_id if it can find a match; otherwise creates a grid and returns its grid_id.
get.grid.id <- function(f, v, con) {
  dim.names <- nc.get.dim.names(f, v)
  dim.axes <- nc.get.dim.axes(f, v)
  dim.axes[is.na(dim.axes)] <- ''

  if(!('X' %in% dim.axes && 'Y' %in% dim.axes))
    return(NA)

  ## Get appropriate X and Y dimensions
  x.dim <- y.dim <- NULL
  if(any(dim.axes == "S")) {
    xy.dims <- nc.get.compress.dims(f, v)
    x.dim <- xy.dims$x.dim
    y.dim <- xy.dims$y.dim
  } else {
    x.dim <- f$dim[[dim.names[dim.axes == 'X']]]
    y.dim <- f$dim[[dim.names[dim.axes == 'Y']]]
  }
  evenly.spaced.y <- nc.is.regular.dimension(y.dim$vals)

  ## Bounds of the domain.
  xc.res <- get.f.step.size(x.dim$vals, mean)
  yc.res <- get.f.step.size(y.dim$vals, mean)
  xc.origin <- x.dim$vals[1]
  yc.origin <- y.dim$vals[1]
  xc.size <- x.dim$len
  yc.size <- y.dim$len

  ## Then, check for the grid based on based on that type.
  grid.diff.fraction <- 0.000001
  query <- paste("SELECT grid_id ",
                 "FROM grids WHERE",
                 " ABS((xc_grid_step - ", xc.res, ") / xc_grid_step) < ", grid.diff.fraction,
                 " AND ABS((yc_grid_step - ", yc.res, ") / yc_grid_step) < ", grid.diff.fraction,
                 ifelse((xc.origin==0),
                        " AND xc_origin==0",
                        paste(" AND ABS((xc_origin - ", xc.origin, ") / xc_origin) < ", grid.diff.fraction, sep="")),
                 ifelse((yc.origin==0),
                        " AND yc_origin==0",
                        paste(" AND ABS((yc_origin - ", yc.origin, ") / yc_origin) < ", grid.diff.fraction, sep="")),
                 " AND xc_count=", xc.size, " and yc_count=", yc.size,
                 " AND evenly_spaced_y=", dbQuote(con, evenly.spaced.y), ";", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  grid.id <- fetch(result, -1)

  ## Check to see whether we have a grid to add
  if(nrow(grid.id) == 0) {
    ## If the grid can't be found, add it and return the ID.
    cell.avg.area.sq.km <- get.cell.avg.area(x.dim, y.dim)

    ## Get units
    query <- paste("INSERT INTO grids(xc_grid_step, yc_grid_step, ",
                       "xc_origin, yc_origin, ",
                       "xc_count, yc_count, ",
                       "cell_avg_area_sq_km, ",
                       "evenly_spaced_y, ",
                       "xc_units, ",
                       "yc_units) ",
                   "VALUES(", paste(xc.res, yc.res,
                                    xc.origin, yc.origin,
                                    xc.size, yc.size,
                                    cell.avg.area.sq.km,
                                    dbQuote(con, evenly.spaced.y),
                                    shQuote(x.dim$units),
                                    shQuote(y.dim$units), sep=","),
                   ")", sep="")
    ##print(query)
    grid.id <- do.insert(con, query, 'grid_id')

    if(!evenly.spaced.y) {
      ## Insert grid data
      y.bnds <- get.bnds.center.array(f, y.dim)
      query <- paste("INSERT INTO y_cell_bounds(grid_id, top_bnd, y_center, bottom_bnd) ",
                     "VALUES(", do.call(paste,
                                        c(list(),
                                          apply(y.bnds, 2, function(x) {
                                            paste(grid.id, x[1], x[2], x[3], sep=",")
                                          }), sep="),(")),
                     ")", sep="")
      ##print(query)
      result <- dbSendQuery(con, query)
    }
  }
  return(grid.id[1, 1])
}

## Finds and returns an emission_id if it can find a match; otherwise
## creates an emissions object and returns its emission_id.
get.emission.id <- function(f, con) {
  meta <- get.file.metadata(f)

  query <- paste("SELECT emission_id ",
                 "FROM emissions ",
                 "WHERE emission_short_name='", meta["emissions"], "';", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  emission.id <- fetch(result, -1)
  if(nrow(emission.id) == 0) {
    query <- paste("INSERT INTO emissions(emission_short_name) ",
                   "VALUES('", meta["emissions"], "') ", sep="")
    emission.id <- do.insert(con, query, 'emission_id')
  }

  return(emission.id[1, 1])
}

## Finds and returns an emission_id if it can find a match; otherwise creates a models object and returns its emission_id.
get.model.id <- function(f, con) {
  meta <- get.file.metadata(f)
  v.list <- nc.get.variable.list(f)
  proj4.string <- if(length(v.list) > 0) nc.get.proj4.string(f, v.list[1]) else ""
  
  ## Really rudimentary GCM/RCM detection.
  is.rcm <- meta["project"] == "NARCCAP" || (meta["project"] != "IPCC Fourth Assessment" && meta["project"] != "CMIP5" && !is.null(proj4.string) && nchar(proj4.string) != 0)
  model.type <- ifelse(is.rcm, "RCM", "GCM")
  
  query <- paste("SELECT model_id FROM models ",
                 "WHERE model_short_name='", meta["model"], "';", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  model.id <- fetch(result, -1)
  if(nrow(model.id) == 0) {
    query <- paste("INSERT INTO models(model_short_name, type, model_organization) ",
                   "VALUES(", paste(shQuote(meta["model"]),
                                    shQuote(model.type),
                                    shQuote(meta["institution"]), sep=","),
                   ")", sep="")
    ##print(query)
    model.id <- do.insert(con, query, 'model_id')
  }

  return(model.id[1, 1])
}

## Finds and returns a run_id if it can find a match; otherwise creates a run and returns its run_id.
## Cannot determine driving_run, initialized_from in this (first) pass. Likewise initialized_from. Lacking IDs in database.
get.run.id <- function(f, con) {
  meta <- get.file.metadata(f)

  emission.id <- get.emission.id(f, con)
  model.id <- get.model.id(f, con)
  
  query <- paste("SELECT run_id FROM runs ",
                 "WHERE run_name='", meta["run"], "' ",
                 "AND emission_id=", emission.id,
                 " AND model_id=", model.id, ";", sep="")
  ##print(query)
  result <- dbSendQuery(con, query)
  run.id <- fetch(result, -1)
  if(nrow(run.id) == 0) {
    query <- paste("INSERT INTO runs(run_name, emission_id, model_id, project) ",
                   "VALUES(", paste(shQuote(meta["run"]),
                                    emission.id,
                                    model.id,
                                    shQuote(meta["project"]), sep=",")
                   , ")", sep="")
    ##print(query)
    run.id <- do.insert(con, query, 'run_id')
  }
  return(run.id[1, 1])
}

get.climatology.bounds.var.name <- function(f) {
  axes <- nc.get.dim.axes(f)
  axes <- axes[!is.na(axes)]
  time.axis.name <- names(axes)[axes[!is.na(axes)] == "T"]
  clim.att <- ncatt_get(f, time.axis.name, "climatology")
  if(clim.att$hasatt)
    return(clim.att$value)
  else
    return(NULL)
}
  
## Returns TRUE if the netcdf metadata provided indicates that the data consists of a multi-year mean.
## May need inference from filename as to time period. (Will see).
is.multi.year.mean <- function(f) {
  if(!is.null(get.climatology.bounds.var.name(f)))
    return(TRUE)
  else
    return(FALSE)
}

## Returns the appropriate time resolution string for the given data.
get.time.resolution <- function(f, time.series) {
  if(is.multi.year.mean(f))
    return("other")
  
  step.size.seconds <- get.f.step.size(unclass(time.series), median)
  return(get.time.resolution.string.from.seconds(step.size.seconds))
}

## Gets string given resolution in seconds
get.time.resolution.string.from.seconds <- function(seconds) {
  return(switch(as.character(seconds), "60"="1-minute", "120"="2-minute", "300"="5-minute", "900"="15-minute", "1800"="30-minute", "3600"="1-hourly", "10800"="3-hourly", "21600"="6-hourly", "43200"="12-hourly", "86400"="daily", "2678400"="monthly", "2635200"="monthly", "2592000"="monthly", "31536000"="yearly", "31104000"="yearly", "other"))
}
