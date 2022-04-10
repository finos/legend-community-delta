package org.finos.legend.spark

import org.finos.legend.sdlc.serialization.EntityLoader
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.Path
import scala.collection.JavaConverters.asScalaIteratorConverter

object LegendFileLoader {

  private final val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  private def getPath(resourceDirectory: String): Path = {
    val file = new File(resourceDirectory)
    if (!file.exists()) throw new IllegalAccessException(s"File $resourceDirectory does not exist")
    if (!file.isDirectory) throw new IllegalAccessException("Not a directory: " + resourceDirectory)
    file.toPath
  }

  def loadResources(resourceName: String): Legend = {
    val path = getPath(resourceName)
    val entityLoader: EntityLoader = EntityLoader.newEntityLoader(path)
    val entities = entityLoader.getAllEntities.iterator().asScala.map(e => (e.getPath, e)).toMap
    val legend = new Legend(entities)
    if (legend.getEntityNames.isEmpty) throw new IllegalAccessException("Could not find any PURE entity to load from classpath")
    LOGGER.info(s"Loaded ${legend.getEntityNames.size} entities")
    legend
  }
}
