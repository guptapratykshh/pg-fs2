package fs2
package io
package process

import cats.effect.IO
import scala.concurrent.duration._

class ProcessCancelHangSuite extends Fs2Suite {
  test("cancel does not hang on SIGTERM intercept") {
    val bashCmd = "trap 'echo TERM' TERM; while true; do sleep 1; done"
    ProcessBuilder("bash", "-c", bashCmd)
      .spawn[IO]
      .use(_ => IO.never)
      .timeout(1.second)
      .handleErrorWith(_ => IO.unit)
      .timeout(3.seconds)
  }
}
