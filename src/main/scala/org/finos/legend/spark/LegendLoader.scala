package org.finos.legend.spark

import java.nio.file.Path

import org.finos.legend.sdlc.serialization.EntityLoader

import scala.collection.JavaConverters._

trait LegendLoader {

  def getPath(resourceDirectory: String): Path

  def loadResources(resourceName: String): Legend = {
    val path = getPath(resourceName)
    val entityLoader: EntityLoader = EntityLoader.newEntityLoader(path)
    val entities = entityLoader.getAllEntities.iterator().asScala.map(e => (e.getPath, e)).toMap
    new Legend(entities)
  }
}
