/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2
package io
package net
package quic

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all._
import com.comcast.ip4s.{Host, IpAddress, Port, SocketAddress}

import java.util.concurrent.ConcurrentHashMap

private[net] final case class QuicServerState[F[_]](
    address: SocketAddress[IpAddress],
    connectionQueue: Queue[F, quic.QuicSocket[F]]
)

private[net] object QuicServerRegistry {
  private val servers = new ConcurrentHashMap[SocketAddress[IpAddress], QuicServerState[Any]]()
  
  def get[F[_]](address: SocketAddress[IpAddress]): Option[QuicServerState[F]] =
    Option(servers.get(address)).map(_.asInstanceOf[QuicServerState[F]])
  
  def put[F[_]](address: SocketAddress[IpAddress], state: QuicServerState[F]): Unit =
    servers.put(address, state.asInstanceOf[QuicServerState[Any]])
  
  def remove(address: SocketAddress[IpAddress]): Unit =
    servers.remove(address)
  
  def containsKey(address: SocketAddress[IpAddress]): Boolean =
    servers.containsKey(address)
}
import scala.jdk.CollectionConverters._

/** JVM implementation of QUIC sockets provider.
  *
  * This implementation simulates QUIC protocol behavior using in-memory
  * communication channels. It provides:
  * - Connection establishment and management
  * - Bidirectional stream multiplexing
  * - Unreliable datagram support
  * - Multiple concurrent connections
  *
  * Note: This is a functional simulation for testing and demonstration.
  * For production use with real network QUIC, a native library integration
  * (like netty-incubator-codec-quic or quiche) would be required.
  */
private[net] final class JvmQuicSocketsProvider[F[_]](implicit F: Async[F], dns: com.comcast.ip4s.Dns[F])
    extends QuicSocketsProvider[F] {

  override def connectQuic(
      address: SocketAddress[Host],
      options: List[SocketOption]
  ): Resource[F, quic.QuicSocket[F]] =
    Resource.eval(address.host.resolve[F]).flatMap { host =>
      val targetAddress = SocketAddress(host, address.port)
      Resource.make(createClientConnection(targetAddress, options))(_ => F.unit)
    }

  override def bindQuic(
      address: SocketAddress[Host],
      options: List[SocketOption]
  ): Resource[F, quic.QuicServerSocket[F]] =
    Resource.eval(address.host.resolve[F]).flatMap { host =>
      val bindAddress =
        if (host.isWildcard) SocketAddress(IpAddress.fromBytes(Array(127, 0, 0, 1)).get, address.port)
        else SocketAddress(host, address.port)
      Resource.make(createServer(bindAddress, options))(_ => 
        F.delay(QuicServerRegistry.remove(bindAddress)).void
      )
    }

  private def createClientConnection(
      remoteAddress: SocketAddress[IpAddress],
      options: List[SocketOption]
  ): F[quic.QuicSocket[F]] =
    for {
      // Find the server to connect to
      serverStateOpt <- F.delay(QuicServerRegistry.get[F](remoteAddress))
      serverState <- serverStateOpt match {
        case Some(state) => F.pure(state)
        case None =>
          F.raiseError[QuicServerState[F]](
            new java.net.ConnectException(
              s"QUIC server not available at $remoteAddress"
            )
          )
      }

      // Create local endpoint
      localPort <- F.delay(Port.fromInt(10000 + scala.util.Random.nextInt(50000)).get)
      localAddress = SocketAddress(IpAddress.fromBytes(Array(127, 0, 0, 1)).get, localPort)

      // Create paired stream buffers for bidirectional communication
      clientToServerBuffers <- Queue.bounded[F, Option[Chunk[Byte]]](64)
      serverToClientBuffers <- Queue.bounded[F, Option[Chunk[Byte]]](64)

      // Create datagram support
      clientDatagramQueue <- Queue.bounded[F, Datagram](64)
      serverDatagramQueue <- Queue.bounded[F, Datagram](64)

      clientDgSocket <- createDatagramSocket(localAddress, remoteAddress, clientDatagramQueue, serverDatagramQueue)
      serverDgSocket <- createDatagramSocket(remoteAddress, localAddress, serverDatagramQueue, clientDatagramQueue)

      clientDgRef <- Ref[F].of[DatagramSocket[F]](clientDgSocket)
      serverDgRef <- Ref[F].of[DatagramSocket[F]](serverDgSocket)

      // Create client and server sockets with linked streams
      clientSocket <- QuicSocketImpl.withLinkedStreams[F](
        localAddress,
        remoteAddress,
        clientToServerBuffers,
        serverToClientBuffers,
        clientDgRef
      )

      serverSocket <- QuicSocketImpl.withLinkedStreams[F](
        remoteAddress,
        localAddress,
        serverToClientBuffers,
        clientToServerBuffers,
        serverDgRef
      )

      // Register connection with server
      _ <- serverState.connectionQueue.offer(serverSocket)

    } yield clientSocket

  private def createServer(
      address: SocketAddress[IpAddress],
      options: List[SocketOption]
  ): F[quic.QuicServerSocket[F]] =
    for {
      // If port is 0, assign a random port
      actualAddress <- if (address.port.value == 0) {
        F.delay {
          val randomPort = Port.fromInt(10000 + scala.util.Random.nextInt(50000)).get
          SocketAddress(address.host, randomPort)
        }
      } else {
        F.pure(address)
      }
      
      // Check if address is already bound
      alreadyBound <- F.delay(QuicServerRegistry.containsKey(actualAddress))
      _ <- if (alreadyBound) {
        F.raiseError[Unit](
          new java.net.BindException(
            s"QUIC server already bound to $actualAddress"
          )
        )
      } else F.unit

      // Create connection queue
      connectionQueue <- Queue.bounded[F, quic.QuicSocket[F]](64)

      // Register server
      serverState = QuicServerState(actualAddress, connectionQueue)
      _ <- F.delay(QuicServerRegistry.put(actualAddress, serverState))

      // Create server socket
      serverSocket <- QuicServerSocketImpl[F](actualAddress, connectionQueue)

    } yield serverSocket

  private def createDatagramSocket(
      localAddr: SocketAddress[IpAddress],
      remoteAddr: SocketAddress[IpAddress],
      incomingQueue: Queue[F, Datagram],
      outgoingQueue: Queue[F, Datagram]
  ): F[DatagramSocket[F]] =
    F.delay {
      new DatagramSocket[F] {
        override def address: com.comcast.ip4s.GenSocketAddress = localAddr

        override def supportedOptions: F[Set[SocketOption.Key[?]]] = F.pure(Set.empty)
        override def getOption[A](key: SocketOption.Key[A]): F[Option[A]] = F.pure(None)
        override def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] = F.unit

        override def readGen: F[fs2.io.net.GenDatagram] =
          incomingQueue.take.asInstanceOf[F[fs2.io.net.GenDatagram]]

        override def connect(address: com.comcast.ip4s.GenSocketAddress): F[Unit] = F.unit
        override def disconnect: F[Unit] = F.unit

        override def read: F[Datagram] = incomingQueue.take

        override def reads: Stream[F, Datagram] =
          Stream.fromQueueUnterminated(incomingQueue)

        override def write(bytes: Chunk[Byte]): F[Unit] =
          outgoingQueue.offer(Datagram(remoteAddr, bytes))

        override def write(bytes: Chunk[Byte], address: com.comcast.ip4s.GenSocketAddress): F[Unit] =
          address match {
            case sa: SocketAddress[_] => outgoingQueue.offer(Datagram(sa.asInstanceOf[SocketAddress[IpAddress]], bytes))
            case _ => F.raiseError(new java.io.IOException("Invalid address type for QUIC datagram"))
          }

        override def writes: Pipe[F, Datagram, Nothing] =
          _.evalMap(d => outgoingQueue.offer(d)).drain

        override def localAddress: F[SocketAddress[IpAddress]] =
          F.pure(localAddr)

        override def join(
            join: com.comcast.ip4s.MulticastJoin[IpAddress],
            interface: com.comcast.ip4s.NetworkInterface
        ): F[GroupMembership] =
          F.raiseError(new UnsupportedOperationException("Multicast not supported in QUIC datagrams"))

        override def join(
            join: com.comcast.ip4s.MulticastJoin[IpAddress],
            interface: DatagramSocket.NetworkInterface
        ): F[GroupMembership] =
          F.raiseError(new UnsupportedOperationException("Multicast not supported in QUIC datagrams"))
      }
    }
}
