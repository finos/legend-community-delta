package org.finos.legend.spark

import java.net.{URISyntaxException, URL}
import java.nio.file.{Files, Path, Paths}
import java.util.Objects

object LegendClasspathLoader extends LegendLoader {

  @throws[URISyntaxException]
  @Override
  def getPath(resourceName: String): Path = {
    val url: URL = Objects.requireNonNull(this.getClass.getClassLoader.getResource(resourceName))
    val directory: Path = Paths.get(url.toURI)
    if (!Files.isDirectory(directory)) throw new IllegalAccessException("Not a directory: " + directory)
    directory
  }

}
