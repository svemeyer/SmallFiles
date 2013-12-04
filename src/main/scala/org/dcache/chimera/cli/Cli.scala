package org.dcache.chimera.cli

import com.martiansoftware.nailgun._
import org.dcache.chimera.FileSystemProvider

object Cli {
  
  def getCommand(cmd : String, args : Array[String]) : ChimeraCommand = {
    val cmdargs = args.slice(FsFactory.ARGC, args.length)
    var provider: FileSystemProvider = FsProviderSingleton.get(args)
    cmd.toLowerCase match {
      case "cchecksum"  => new NotImplemented(provider, cmdargs)
      case "cchgrp"     => new NotImplemented(provider, cmdargs)
      case "cchmod"     => new NotImplemented(provider, cmdargs)
      case "cchown"     => new NotImplemented(provider, cmdargs)
      case "cgetfacl"   => new NotImplemented(provider, cmdargs)
      case "cln"        => new CLn(provider, cmdargs)
      case "cls"        => new NotImplemented(provider, cmdargs)
      case "clstag"     => new NotImplemented(provider, cmdargs)
      case "cmkdir"     => new CMkdir(provider, cmdargs)
      case "cnameof"    => new CNameof(provider, cmdargs)
      case "cparent"    => new CParent(provider, cmdargs)
      case "cpathof"    => new CPathof(provider, cmdargs)
      case "creadlevel" => new CReadLevel(provider, cmdargs)
      case "creadtag"   => new CReadTag(provider, cmdargs)
      case "crm"        => new CRm(provider, cmdargs)
      case "crmtag"     => new NotImplemented(provider, cmdargs)
      case "csetfacl"   => new NotImplemented(provider, cmdargs)
      case "cstat"      => new CStat(provider, cmdargs)
      case "ctouch"     => new CTouch(provider, cmdargs)
      case "cwritedata" => new NotImplemented(provider, cmdargs)
      case "cwritelevel"=> new CWriteLevel(provider, cmdargs)
      case "cwritetag"  => new CWriteTag(provider, cmdargs)
      case "sfput"      => new SmallFilePut(provider, cmdargs)
      case _ => throw new SystemExitException(4, "Unknown command")
    }
  }

  def nailMain(context : NGContext) {
    try {
      getCommand(context.getCommand, context.getArgs).run()
      context.exit(0)
    } catch {
      case e:SystemExitException => {
        System.err.println(e.toString)
        context.exit(e.exitCode)
      }
      case e: Throwable => System.err.println(e.getMessage)
    }
  }

  def nailShutdown(server : NGServer) {

  }
}
