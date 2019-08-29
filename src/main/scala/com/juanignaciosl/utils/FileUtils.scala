package com.juanignaciosl.utils

import java.io.File

object FileUtils {

  def resourcePath(resourceFile: String): String = {
    val resource = this.getClass.getClassLoader.getResource(resourceFile)
    if (resource == null) sys.error(s"$resourceFile not found")
    new File(resource.toURI).getPath
  }

}
