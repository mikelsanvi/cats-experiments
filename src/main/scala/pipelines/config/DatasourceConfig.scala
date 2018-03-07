/*
 * Copyright 2018, Radius Intelligence, Inc.
 * All Rights Reserved
 */

package pipelines.config

import java.time.LocalDate

import pipelines.{DataSourceDescriptor, SourceOrigin}

case class DatasourceConfig(
    sourceOriginDefaultPaths: Map[SourceOrigin, String],
    dataSourcePathsOverride: Map[String, String],
    horizon: Option[LocalDate]) {

  def path(dataset: DataSourceDescriptor[_]): String = {
    dataSourcePathsOverride.getOrElse(dataset.name, {
      val basePath = sourceOriginDefaultPaths(dataset.sourceOrigin)
      s"$basePath/${dataset.path}"
    })
  }
}


