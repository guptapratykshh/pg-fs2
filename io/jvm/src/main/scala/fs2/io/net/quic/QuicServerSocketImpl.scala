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
import com.comcast.ip4s.{IpAddress, SocketAddress}

/** Simulated QUIC server socket implementation.
  *
  * This implementation accepts incoming QUIC connections and provides:
  * - Connection acceptance from multiple clients
  * - Per-connection isolation
  * - Proper resource management
  */
private[net] final class QuicServerSocketImpl[F[_]](
    val address: SocketAddress[IpAddress],
    connectionQueue: Queue[F, quic.QuicSocket[F]],
    closed: Ref[F, Boolean]
)(implicit F: Async[F])
    extends quic.QuicServerSocket[F] {

  override def supportedOptions: F[Set[SocketOption.Key[?]]] =
    F.pure(Set.empty)

  override def getOption[A](key: SocketOption.Key[A]): F[Option[A]] =
    F.pure(None)

  override def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] =
    F.unit

  override def connections: Stream[F, quic.QuicSocket[F]] =
    Stream.repeatEval(
      closed.get.flatMap { isClosed =>
        if (isClosed) F.raiseError[quic.QuicSocket[F]](new java.io.IOException("QUIC server closed"))
        else connectionQueue.take
      }
    )
}

private[net] object QuicServerSocketImpl {
  def apply[F[_]: Async](
      address: SocketAddress[IpAddress],
      connectionQueue: Queue[F, quic.QuicSocket[F]]
  ): F[QuicServerSocketImpl[F]] =
    Ref[F].of(false).map { closed =>
      new QuicServerSocketImpl[F](address, connectionQueue, closed)
    }
}
