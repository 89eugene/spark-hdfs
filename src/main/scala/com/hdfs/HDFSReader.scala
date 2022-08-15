package com.hdfs

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object HDFSReader  extends App {


  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val stagePath = new Path(new URI("/stage"))
  val odsPath = new Path(new URI("/ods"))

  val status = fileSystem.listStatus(stagePath)

  def writeToFile(oldPath: Path): Unit = {

    val files: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(oldPath, true);

    var needCreateNewFile = true
    var newFile: FSDataOutputStream = null

    while (files.hasNext) {
      val next: LocatedFileStatus = files.next()
      if (next.getPath.toString.endsWith(".csv")) {
        if(needCreateNewFile){
          val newPath = odsPath.suffix("/" + oldPath.getName + "/part-0000.csv")
          newFile = fileSystem.create(newPath)
          needCreateNewFile = false
        }
        val openedFile: FSDataInputStream = fileSystem.open(next.getPath)
        newFile.write(openedFile.readAllBytes())
        openedFile.close()
        fileSystem.delete(next.getPath, false)
      }
    }

    if(newFile != null) {
      newFile.close()
    }
  }

  try {
    status.foreach(x => {
      val oldPath = x.getPath
      val newPathFolder = odsPath.suffix("/" + oldPath.getName)
      fileSystem.mkdirs(newPathFolder)
      writeToFile(oldPath)
    })
  }finally{
      fileSystem.close()
    }

}
