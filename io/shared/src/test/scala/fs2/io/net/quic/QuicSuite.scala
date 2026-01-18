package fs2
package io
package net
package quic

import cats.effect.IO
import cats.syntax.all._
import com.comcast.ip4s._
import scala.concurrent.duration._

class QuicSuite extends Fs2Suite {

  group("QUIC client/server connection") {
    test("connect and close") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        Network[IO].connectQuic(boundAddress).use { client =>
          IO(client.remoteAddress).map { remoteAddr =>
            assertEquals(remoteAddr.asIpUnsafe, boundAddress)
          }
        }
      }
    }

    test("server accepts connections") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        val clientConnect = Network[IO].connectQuic(boundAddress).use { _ =>
          IO.sleep(100.millis)
        }
        
        val serverAccept = server.connections.take(1).compile.drain
        
        (clientConnect, serverAccept).parTupled.void
      }
    }

    test("multiple connections") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        val clients = List.fill(5) {
          Network[IO].connectQuic(boundAddress).use { _ =>
            IO.sleep(50.millis)
          }
        }
        
        val serverAccept = server.connections.take(5).compile.drain
        
        (clients.parSequence_, serverAccept).parTupled.void
      }
    }

    test("connection addresses") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        Network[IO].connectQuic(boundAddress).use { client =>
          server.connections.head.compile.lastOrError.flatMap { serverConn =>
            IO {
              assertEquals(client.remoteAddress.asIpUnsafe.host, boundAddress.host)
              assertEquals(serverConn.remoteAddress.asIpUnsafe.host, client.address.asIpUnsafe.host)
            }
          }
        }
      }
    }
  }

  group("QUIC bidirectional streams") {
    test("open stream") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        Network[IO].connectQuic(boundAddress).use { client =>
          client.openStream().use { stream =>
            IO(stream).map { s =>
              assertEquals(s.peerAddress.asIpUnsafe.host, boundAddress.host)
            }
          }
        }
      }
    }



  }

  group("QUIC datagrams") {
    test("send and receive datagram") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      val testData = Chunk.array("Datagram!".getBytes)
      
      Network[IO].bindQuic(serverAddress).use { server =>
        val boundAddress = server.address.asIpUnsafe
        
        val clientSend = Network[IO].connectQuic(boundAddress).use { client =>
          client.datagrams.write(testData)
        }
        
        val serverReceive = server.connections.head.compile.lastOrError.flatMap { serverConn =>
          serverConn.datagrams.reads.head.compile.lastOrError.map { datagram =>
            assertEquals(datagram.bytes, testData)
          }
        }
        
        (clientSend, serverReceive).parTupled.void
      }
    }

    test("bidirectional datagrams") {
      val serverAddress = SocketAddress(ip"127.0.0.1", port"0")
      val clientData = Chunk.array("Client Datagram".getBytes)
      val serverData = Chunk.array("Server Datagram".getBytes)
      
      Network[IO].bindQuic(serverAddress).use { serverSocket =>
        val boundAddress = serverSocket.address.asIpUnsafe
        
        val client = Network[IO].connectQuic(boundAddress).use { clientConn =>
          for {
            _ <- clientConn.datagrams.write(clientData)
            received <- clientConn.datagrams.reads.head.compile.lastOrError
          } yield received
        }
        
        val server = serverSocket.connections.head.compile.lastOrError.flatMap { serverConn =>
          for {
            received <- serverConn.datagrams.reads.head.compile.lastOrError
            _ <- serverConn.datagrams.write(serverData)
          } yield received
        }
        
        (client, server).parTupled.map { case (clientReceived, serverReceived) =>
          assertEquals(serverReceived.bytes, clientData)
          assertEquals(clientReceived.bytes, serverData)
        }
      }
    }
  }

  group("QUIC error handling") {
    test("connect to non-existent server") {
      val address = SocketAddress(ip"127.0.0.1", port"9999")
      
      Network[IO].connectQuic(address).use_.intercept[java.net.ConnectException]
    }

    test("bind to same address twice") {
      val address = SocketAddress(ip"127.0.0.1", port"0")
      
      Network[IO].bindQuic(address).use { server1 =>
        val boundAddress = server1.address
        Network[IO].bindQuic(boundAddress).use_.intercept[java.net.BindException]
      }
    }

    test("QUIC over Unix sockets throws UnsupportedOperationException") {
      val address = UnixSocketAddress("/tmp/test.sock")
      
      Network[IO].connectQuic(address).use_.intercept[UnsupportedOperationException]
    }
  }
}
