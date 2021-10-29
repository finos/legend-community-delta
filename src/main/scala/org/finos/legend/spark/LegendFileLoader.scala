package org.finos.legend.spark

import java.io.File
import java.net.URISyntaxException
import java.nio.file.Path

object LegendFileLoader extends LegendLoader {

  @throws[URISyntaxException]
  @Override
  def getPath(resourceDirectory: String): Path = {
    val file = new File(resourceDirectory)
    if (!file.exists()) throw new IllegalAccessException(s"File $resourceDirectory does not exist")
    if (!file.isDirectory) throw new IllegalAccessException("Not a directory: " + resourceDirectory)
    file.toPath
  }

}
