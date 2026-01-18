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
import cats.effect.std.{Queue, Semaphore}
import cats.syntax.all._
import com.comcast.ip4s.{GenSocketAddress, IpAddress, SocketAddress}

import java.util.concurrent.atomic.AtomicLong

/** Simulated QUIC socket implementation.
  *
  * This implementation provides a functional QUIC connection with:
  * - Bidirectional stream multiplexing
  * - Unreliable datagram support
  * - Flow control simulation
  * - Proper resource cleanup
  */
private[net] final class QuicSocketImpl[F[_]](
    val address: SocketAddress[IpAddress],
    val remoteAddress: SocketAddress[IpAddress],
    streamQueue: Queue[F, Socket[F]],
    datagramSocketRef: Ref[F, DatagramSocket[F]],
    streamManager: QuicStreamManager[F],
    closed: Ref[F, Boolean]
)(implicit F: Async[F])
    extends quic.QuicSocket[F] {

  override def supportedOptions: F[Set[SocketOption.Key[?]]] =
    F.pure(Set.empty)

  override def getOption[A](key: SocketOption.Key[A]): F[Option[A]] =
    F.pure(None)

  override def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] =
    F.unit

  override def openStream(options: List[SocketOption]): Resource[F, Socket[F]] =
    Resource.make(
      closed.get.flatMap { isClosed =>
        if (isClosed) F.raiseError[Socket[F]](new java.io.IOException("QUIC connection closed"))
        else streamManager.createBidirectionalStream(address, remoteAddress)
      }
    )(socket => socket.endOfOutput *> socket.endOfInput)

  override def streams: Stream[F, Socket[F]] =
    Stream.fromQueueUnterminated(streamQueue)

  override def datagrams: DatagramSocket[F] =
    new DatagramSocket[F] {
      override def address: GenSocketAddress = QuicSocketImpl.this.address

      override def supportedOptions: F[Set[SocketOption.Key[?]]] = F.pure(Set.empty)
      override def getOption[A](key: SocketOption.Key[A]): F[Option[A]] = F.pure(None)
      override def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] = F.unit

      override def readGen: F[GenDatagram] =
        datagramSocketRef.get.flatMap(_.readGen)

      override def connect(address: GenSocketAddress): F[Unit] =
        datagramSocketRef.get.flatMap(_.connect(address))

      override def disconnect: F[Unit] =
        datagramSocketRef.get.flatMap(_.disconnect)

      override def read: F[Datagram] =
        datagramSocketRef.get.flatMap(_.read)

      override def reads: Stream[F, Datagram] =
        Stream.eval(datagramSocketRef.get).flatMap(_.reads)

      override def write(bytes: Chunk[Byte]): F[Unit] =
        datagramSocketRef.get.flatMap(_.write(bytes))

      override def write(bytes: Chunk[Byte], address: GenSocketAddress): F[Unit] =
        datagramSocketRef.get.flatMap(_.write(bytes, address))

      override def writes: Pipe[F, Datagram, Nothing] =
        _.evalMap(d => datagramSocketRef.get.flatMap(_.write(d))).drain

      override def localAddress: F[SocketAddress[IpAddress]] =
        F.pure(QuicSocketImpl.this.address)

      override def join(
          join: com.comcast.ip4s.MulticastJoin[IpAddress],
          interface: com.comcast.ip4s.NetworkInterface
      ): F[GroupMembership] =
        datagramSocketRef.get.flatMap { sock =>
          sock.join(join, interface).asInstanceOf[F[GroupMembership]]
        }

      override def join(
          join: com.comcast.ip4s.MulticastJoin[IpAddress],
          interface: DatagramSocket.NetworkInterface
      ): F[GroupMembership] =
        datagramSocketRef.get.flatMap { sock =>
          sock.join(join, interface).asInstanceOf[F[GroupMembership]]
        }
    }
}

private[net] object QuicSocketImpl {
  def apply[F[_]: Async](
      localAddress: SocketAddress[IpAddress],
      remoteAddress: SocketAddress[IpAddress],
      streamQueue: Queue[F, Socket[F]],
      datagramSocketRef: Ref[F, DatagramSocket[F]],
      streamManager: QuicStreamManager[F]
  ): F[QuicSocketImpl[F]] =
    Ref[F].of(false).map { closed =>
      new QuicSocketImpl[F](
        localAddress,
        remoteAddress,
        streamQueue,
        datagramSocketRef,
        streamManager,
        closed
      )
    }

  def withLinkedStreams[F[_]: Async](
      localAddress: SocketAddress[IpAddress],
      remoteAddress: SocketAddress[IpAddress],
      readBuffer: Queue[F, Option[Chunk[Byte]]],
      writeBuffer: Queue[F, Option[Chunk[Byte]]],
      datagramSocketRef: Ref[F, DatagramSocket[F]]
  ): F[QuicSocketImpl[F]] =
    for {
      closed <- Ref[F].of(false)
      streamQueue <- Queue.bounded[F, Socket[F]](64)
      streamManager <- Async[F].delay(new LinkedQuicStreamManager[F](readBuffer, writeBuffer))
    } yield new QuicSocketImpl[F](
      localAddress,
      remoteAddress,
      streamQueue,
      datagramSocketRef,
      streamManager,
      closed
    )
}

/** Manager for QUIC streams within a connection */
private[net] class QuicStreamManager[F[_]](implicit F: Async[F]) {
  private val streamIdGenerator = new AtomicLong(0L)

  def createBidirectionalStream(
      localAddress: SocketAddress[IpAddress],
      remoteAddress: SocketAddress[IpAddress]
  ): F[Socket[F]] =
    for {
      streamId <- F.delay(streamIdGenerator.incrementAndGet())
      readBuffer <- Queue.bounded[F, Option[Chunk[Byte]]](64)
      writeBuffer <- Queue.bounded[F, Chunk[Byte]](64)
      writeSem <- Semaphore[F](1)
      readSem <- Semaphore[F](1)
      closedRef <- Ref[F].of(false)
      endOfInputRef <- Ref[F].of(false)
      endOfOutputRef <- Ref[F].of(false)
    } yield createStreamSocket(
      localAddress,
      remoteAddress,
      streamId,
      readBuffer,
      writeBuffer,
      writeSem,
      readSem,
      closedRef,
      endOfInputRef,
      endOfOutputRef
    )

  private def createStreamSocket(
      localAddr: SocketAddress[IpAddress],
      remoteAddr: SocketAddress[IpAddress],
      streamId: Long,
      readBuffer: Queue[F, Option[Chunk[Byte]]],
      writeBuffer: Queue[F, Chunk[Byte]],
      writeSem: Semaphore[F],
      readSem: Semaphore[F],
      closedRef: Ref[F, Boolean],
      endOfInputRef: Ref[F, Boolean],
      endOfOutputRef: Ref[F, Boolean]
  ): Socket[F] = new Socket[F] {
    override def address: GenSocketAddress = localAddr
    override def peerAddress: GenSocketAddress = remoteAddr

    override def supportedOptions: F[Set[SocketOption.Key[?]]] = F.pure(Set.empty)
    override def getOption[A](key: SocketOption.Key[A]): F[Option[A]] = F.pure(None)
    override def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] = F.unit

    override def read(maxBytes: Int): F[Option[Chunk[Byte]]] =
      readSem.permit.use { _ =>
        endOfInputRef.get.flatMap { endOfInput =>
          if (endOfInput) {
            // If end of input was signaled, check if there's buffered data
            readBuffer.tryTake.flatMap {
              case Some(Some(chunk)) => F.pure(Some(chunk.take(maxBytes)))
              case Some(None) | None => F.pure(None) // EOF
            }
          } else {
            readBuffer.take.flatMap {
              case Some(chunk) => F.pure(Some(chunk.take(maxBytes)))
              case None =>
                endOfInputRef.set(true) *> F.pure(None)
            }
          }
        }
      }

    override def readN(numBytes: Int): F[Chunk[Byte]] = {
      def go(remaining: Int, acc: List[Chunk[Byte]]): F[Chunk[Byte]] =
        if (remaining <= 0) F.pure(Chunk.concat(acc.reverse))
        else
          read(remaining).flatMap {
            case Some(chunk) =>
              val newRemaining = remaining - chunk.size
              if (newRemaining <= 0) F.pure(Chunk.concat((acc :+ chunk).reverse))
              else go(newRemaining, chunk :: acc)
            case None => F.pure(Chunk.concat(acc.reverse))
          }
      go(numBytes, Nil)
    }

    override def reads: Stream[F, Byte] =
      Stream.repeatEval(read(8192)).unNoneTerminate.flatMap(Stream.chunk)

    override def endOfInput: F[Unit] =
      endOfInputRef.set(true) *> readBuffer.offer(None)

    override def endOfOutput: F[Unit] =
      endOfOutputRef.set(true) *> writeBuffer.offer(Chunk.empty)

    override def write(bytes: Chunk[Byte]): F[Unit] =
      writeSem.permit.use { _ =>
        endOfOutputRef.get.flatMap {
          case true  => F.raiseError(new java.io.IOException("Stream output closed"))
          case false => writeBuffer.offer(bytes)
        }
      }

    override def writes: Pipe[F, Byte, Nothing] =
      _.chunks.evalMap(write).drain

    override def isOpen: F[Boolean] =
      closedRef.get.map(!_)
    override def localAddress: F[SocketAddress[IpAddress]] =
      F.pure(localAddr)
    override def remoteAddress: F[SocketAddress[IpAddress]] =
      F.pure(remoteAddr)
  }
}
