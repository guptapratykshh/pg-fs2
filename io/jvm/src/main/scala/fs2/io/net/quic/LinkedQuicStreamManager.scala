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

import cats.effect.{Async, Ref, Semaphore}
import cats.effect.std.Queue
import java.util.concurrent.atomic.AtomicLong

/** Stream manager that links client and server streams for proper EOF signaling */
private[net] class LinkedQuicStreamManager[F[_]](
    readBuffer: Queue[F, Option[Chunk[Byte]]],
    writeBuffer: Queue[F, Option[Chunk[Byte]]]
)(implicit F: Async[F]) extends QuicStreamManager[F] {
  
  private val streamIdGenerator = new AtomicLong(0L)

  override def createBidirectionalStream(
      localAddress: SocketAddress[IpAddress],
      remoteAddress: SocketAddress[IpAddress]
  ): F[Socket[F]] =
    for {
      streamId <- F.delay(streamIdGenerator.incrementAndGet())
      writeSem <- Semaphore[F](1)
      readSem <- Semaphore[F](1)
      closedRef <- Ref[F].of(false)
      endOfInputRef <- Ref[F].of(false)
      endOfOutputRef <- Ref[F].of(false)
    } yield createLinkedStreamSocket(
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

  private def createLinkedStreamSocket(
      localAddr: SocketAddress[IpAddress],
      remoteAddr: SocketAddress[IpAddress],
      streamId: Long,
      readBuf: Queue[F, Option[Chunk[Byte]]],
      writeBuf: Queue[F, Option[Chunk[Byte]]],
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
            readBuf.tryTake.flatMap {
              case Some(Some(chunk)) => F.pure(Some(chunk.take(maxBytes)))
              case Some(None) | None => F.pure(None) // EOF
            }
          } else {
            readBuf.take.flatMap {
              case Some(chunk) => F.pure(Some(chunk.take(maxBytes)))
              case None => // EOF signal received
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
      endOfInputRef.set(true) *> readBuf.offer(None)

    override def endOfOutput: F[Unit] =
      endOfOutputRef.set(true) *> writeBuf.offer(None)

    override def write(bytes: Chunk[Byte]): F[Unit] =
      writeSem.permit.use { _ =>
        endOfOutputRef.get.flatMap {
          case true  => F.raiseError(new java.io.IOException("Stream output closed"))
          case false => writeBuf.offer(Some(bytes))
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
