package org.finos.legend.spark

import org.finos.legend.sdlc.serialization.EntityLoader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaIteratorConverter

object LegendClasspathLoader {

  private final val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  def loadResources(): Legend = {
    val entityLoader = EntityLoader.newEntityLoader(this.getClass.getClassLoader)
    val entities = entityLoader.getAllEntities.iterator().asScala.map(e => (e.getPath, e)).toMap
    val legend = new Legend(entities)
    if (legend.getEntityNames.isEmpty) throw new IllegalAccessException("Could not find any PURE entity to load from classpath")
    LOGGER.info(s"Loaded ${legend.getEntityNames.size} entities")
    legend
  }
}
