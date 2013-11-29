package org.dcache.chimera.cli

import org.dcache.chimera._
import com.google.common.io.ByteStreams.toByteArray
import java.io.File
import java.security.MessageDigest

abstract class ChimeraCommand(provider : FileSystemProvider, args : Array[String]) {
  def run()
}

class SystemExitException(code: Int, message: String) extends Exception {
  override def toString = "Error " + code + ": " + message
  val exitCode = code
}

trait Arguments {
  def exitOnPrintUsage(usage : String) {
  }

  def checkUsage(args : Array[String], expectedCount : Int, usage : String) {
    val argc = args.length
    if (argc != expectedCount) {
      throw new SystemExitException(4, "Usage: " + this.getClass.getName + " " + FsFactory.USAGE + " " + usage)
    }
  }

  def checkUsage(args : Array[String], minExpectedCount : Int, maxExpectedCount : Int, usage : String) {
    val argc = args.length
    if (argc < minExpectedCount || argc > maxExpectedCount) {
      throw new SystemExitException(4, "Usage: " + this.getClass.getName + " " + FsFactory.USAGE + " " + usage)
    }
  }
}

trait ReadIntoByteArray {
  def getData(read : (Long, Array[Byte], Int, Int) => Int) : String = {
    val data = new Array[Byte](8192)
    if ( read(0, data, 0, data.length) == -1 )
      throw new SystemExitException(1, "")

    val s = new String(data)
    s.substring(0,s.indexOf("\u0000"))
  }
}

trait ExceptionHandling {
  def handleExceptions(block : () => Any) {
    try {
      block()
    } catch {
      case e:FileNotFoundHimeraFsException =>
        throw new SystemExitException(2, "File not found: "+e.getMessage)
      case e:SystemExitException =>
        throw e
      case e:Throwable => throw new SystemExitException(13, e.getMessage)
    }
  }
}

class NotImplemented(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) {
  def run() {
    throw new SystemExitException(404, "Not implemented")
  }
}

class CMkdir(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<chimera path>")
    handleExceptions {
      () => provider.mkdir(args(0))
    }
  }
}

class CNameof(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args)  with Arguments with ReadIntoByteArray with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<pnfsid>")
    val id = args(0)
    handleExceptions {
      () => {
        val inode = new FsInode_NAMEOF(provider, id)
        print(getData(inode.read))
      }
    }
  }
}

class CParent(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ReadIntoByteArray with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<pnfsid>")
    val id = args(0)
    handleExceptions {
      () => {
        val inode = new FsInode_PARENT(provider, id)
        print(getData(inode.read))
      }
    }
  }
}

class CPathof(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ReadIntoByteArray with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<pnfsid>")
    val id = args(0)
    handleExceptions {
      () => {
        val inode = new FsInode_PATHOF(provider, id)
        print(getData(inode.read))
      }
    }
  }
}

class CReadLevel(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ReadIntoByteArray with ExceptionHandling {
  def run() {
    checkUsage(args, 2, "<chimera path> <level>")
    handleExceptions {
      () => {
        val inode = provider.path2inode(args(0))
        val level = Integer.parseInt(args(1))
        print(getData(provider.read(inode, level, _, _, _, _)))
      }
    }
  }
}

class CReadTag(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 2, "<chimera path> <tag>")
    val path = args(0)
    val tagName = args(1)
    handleExceptions {
      () => {
        val inode = provider.path2inode(path)
        val stat = provider.statTag(inode, tagName)
        val data = Array.ofDim[Byte](stat.getSize.toInt)

        provider.getTag(inode, tagName, data, 0, data.length)

        new String(data)
      }
    }
  }
}

class CRm(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<chimera path>")
    handleExceptions {
      () => provider.remove(args(0))
    }
  }
}

class CStat(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<chimera path>")
    val path = args(0)
    handleExceptions{
      () => {
        val inode = provider.path2inode(path)

        println(inode.stat)
      }
    }
  }
}

class CTouch(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 1, "<chimera path>")
    val path = args(0)
    handleExceptions {
      () => provider.createFile(path)
    }
  }
}

class CLn(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling
{
  def run() {
    checkUsage(args, 2, "<pnfsid> <chimera path>")
    val pnfsid = args(0)
    val target = args(1)
    handleExceptions {
      () => {
        val link = new File(target)
        val srcinode = new FsInode(provider, pnfsid)
        val targetbase = provider.path2inode(link.getParent)
        provider.createHLink(targetbase, srcinode, link.getName)
      }
    }
  }
}

class CWriteLevel(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 2, 3, "<chimera path> <level> [<data>]")
    handleExceptions {
      () => {
        val inode = provider.path2inode(args(0))
        val level = Integer.parseInt(args(1))

        provider.createFileLevel(inode, level)

        val data = if (args.length == 2) toByteArray(System.in) else {
          val term = args(2)
          (if (term.endsWith("\n")) term else term+"\n").getBytes
        }

        if (data.length > 0) {
          if ( provider.write(inode, level, 0, data, 0, data.length) == -1 )
            throw new SystemExitException(1, "")
        }
      }
    }
  }
}

class CWriteTag(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling {
  def run() {
    checkUsage(args, 2, 3, "<chimera path> <level> [<data>]")
    handleExceptions{
      () => {
        val inode = provider.path2inode(args(0))
        val tag = args(1)

        try {
          provider.statTag(inode, tag)
        } catch {
          case fnf : FileNotFoundHimeraFsException => {
            provider.createTag(inode, tag)
          }
        }

        val data = if (args.length == 2) toByteArray(System.in) else {
          val term = args(2)
          (if (term.endsWith("\n")) term else term+"\n").getBytes
        }

        if (data.length > 0) {
          provider.setTag(inode, tag, data, 0, data.length)
        }
      }
    }
  }
}

class SmallFilePut(provider : FileSystemProvider, args : Array[String]) extends ChimeraCommand(provider, args) with Arguments with ExceptionHandling with ReadIntoByteArray {
  def run() {
    checkUsage(args, 5, "<dataRoot> <hsmBase> <store> <group> <pnfsid>")
    val dataRoot=args(0)
    val hsmBase=args(1)
    val store = args(2)
    val group = args(3)
    val pnfsid = args(4)

    handleExceptions{
      () => {
        val pathInode = new FsInode_PATHOF(provider, pnfsid)
        val path = getData(pathInode.read)
        val fileDirHash: String = md5sum(new File(path).getParent)
        val requestBase = String.format("%s/%s/requests", dataRoot, hsmBase)
        val requestFlag = String.format("%s/%s/%s/%s/%s", requestBase, store, group, fileDirHash, pnfsid)

        val reply = try {
          val replyInode = provider.path2inode(requestFlag)
          getData(provider.read(replyInode, 5, _, _, _, _))
        } catch { case _:Throwable => "" }

        if (!reply.isEmpty) {
          provider.remove(requestFlag)
          if (reply.startsWith("ERROR")) {
            throw new SystemExitException(10, reply)
          } else {
            println(reply)
          }
        } else if (try { provider.stat(requestFlag); true } catch { case _:Throwable => false }) {
          throw new SystemExitException(2, "Not yet ready")
        } else {
          def tryMkdir(path : String) { try { provider.mkdir(path) } catch { case e:ChimeraFsException => } }
          tryMkdir(requestBase+"/"+store)
          tryMkdir(requestBase+"/"+store+"/"+group)
          tryMkdir(requestBase+"/"+store+"/"+group+"/"+fileDirHash)
          provider.createFile(requestFlag)
          throw new SystemExitException(3, "Request initialized (async)")
        }
      }
    }
  }

  def md5sum(s : String): String = {
    val digester = MessageDigest.getInstance("MD5")
    digester.update(s.getBytes())
    BigInt(1, digester.digest).toString(16)
  }
}
