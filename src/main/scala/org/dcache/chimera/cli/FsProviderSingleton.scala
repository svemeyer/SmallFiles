package org.dcache.chimera.cli

import org.dcache.chimera.{JdbcFs, FileSystemProvider}
import com.jolbox.bonecp.BoneCPDataSource

object FsProviderSingleton {

  val USAGE = "<jdbcDrv> <jdbcUrl> <dbDialect> <dbUser> <dbPass>"
  val ARGC = 5

  private var provider : Option[FileSystemProvider] = None

  def createFileSystem(args : Array[String]) = {
    if (args.length < 5) {
      throw new IllegalArgumentException()
    }

    val jdbcDrv = args(0)
    val jdbcUrl = args(1)
    val dbDialect = args(2)
    val dbUser = args(3)
    val dbPass = args(4)

    Class.forName(jdbcDrv)

    val ds = new BoneCPDataSource()
    ds.setJdbcUrl(jdbcUrl)
    ds.setUsername(dbUser)
    ds.setPassword(dbPass)
    ds.setIdleConnectionTestPeriodInMinutes(60)
    ds.setIdleMaxAgeInMinutes(240)
    ds.setMaxConnectionsPerPartition(2)
    ds.setMinConnectionsPerPartition(1)
    ds.setPartitionCount(10)
    ds.setAcquireIncrement(1)
    ds.setStatementsCacheSize(100)
    ds.setReleaseHelperThreads(0)
    ds.setDisableConnectionTracking(true)
    ds.setDisableJMX(true)
    ds.setStatisticsEnabled(false)

    new JdbcFs(ds, dbDialect)
  }

  def get(args : Array[String]) : FileSystemProvider = {
    provider match {
      case Some(p) => p
      case None => {
        provider = Option(createFileSystem(args))
        provider.get
      }
    }
  }

}