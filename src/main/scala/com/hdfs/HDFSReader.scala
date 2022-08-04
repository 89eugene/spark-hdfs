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

  def createDirectoryAndFile(path: Path): FSDataOutputStream = fileSystem.create(path)

  def writeToFile(newFile: FSDataOutputStream, oldPath: Path): Unit = {

    val files: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(oldPath, true);

    while (files.hasNext) {
      val next: LocatedFileStatus = files.next();
      if (next.getPath.toString.endsWith(".csv")) {
        val openedFile: FSDataInputStream = fileSystem.open(next.getPath)
        newFile.write(openedFile.readAllBytes())
        openedFile.close()
        fileSystem.delete(next.getPath, false)
      }
    }
    newFile.close()
  }

  try {
    status.foreach(x => {
      val oldPath = x.getPath
      val newPath = odsPath.suffix("/" + oldPath.getName + "/part-0000")
      val newFile: FSDataOutputStream = createDirectoryAndFile(newPath)
      writeToFile(newFile, oldPath)
    })
  }finally{
      fileSystem.close()
    }

}
