package com.nikias.mohanmca
import scala.collection.JavaConversions._
import org.apache.spark.deploy.SparkSubmitArguments
import java.io.File
import org.apache.spark.util._
import org.apache.commons.lang3.SystemUtils
import java.io.IOException
import java.io.OutputStream
import java.io.InputStream

/**
 * @author Mohan
 */
object ProcessConstructor {
  def main(args: Array[String]): Unit = {

    if (!sys.env.contains("SPARK_CLASS")) {
      System.err.println("SparkSubmitDriverBootstrapper must be called from `bin/spark-class`!")
      System.exit(1)
    }

    val submitArgs = args
    val runner = sys.env("RUNNER")
    val classpath = sys.env("CLASSPATH")
    val javaOpts = sys.env("JAVA_OPTS")
    val defaultDriverMemory = sys.env("OUR_JAVA_MEM")

    // Spark submit specific environment variables
    val deployMode = sys.env("SPARK_SUBMIT_DEPLOY_MODE")
    val propertiesFile = sys.env("SPARK_SUBMIT_PROPERTIES_FILE")
    val bootstrapDriver = sys.env("SPARK_SUBMIT_BOOTSTRAP_DRIVER")
    val submitDriverMemory = sys.env.get("SPARK_SUBMIT_DRIVER_MEMORY")
    val submitLibraryPath = sys.env.get("SPARK_SUBMIT_LIBRARY_PATH")
    val submitClasspath = sys.env.get("SPARK_SUBMIT_CLASSPATH")
    val submitJavaOpts = sys.env.get("SPARK_SUBMIT_OPTS")

    assume(runner != null, "RUNNER must be set")
    assume(classpath != null, "CLASSPATH must be set")
    assume(javaOpts != null, "JAVA_OPTS must be set")
    assume(defaultDriverMemory != null, "OUR_JAVA_MEM must be set")
    assume(deployMode == "client", "SPARK_SUBMIT_DEPLOY_MODE must be \"client\"!")
    assume(propertiesFile != null, "SPARK_SUBMIT_PROPERTIES_FILE must be set")
    assume(bootstrapDriver != null, "SPARK_SUBMIT_BOOTSTRAP_DRIVER must be set")

    // Parse the properties file for the equivalent spark.driver.* configs
    val properties = SparkSubmitArguments.getPropertiesFromFile(new File(propertiesFile)).toMap
    val confDriverMemory = properties.get("spark.driver.memory")
    val confLibraryPath = properties.get("spark.driver.extraLibraryPath")
    val confClasspath = properties.get("spark.driver.extraClassPath")
    val confJavaOpts = properties.get("spark.driver.extraJavaOptions")

    // Favor Spark submit arguments over the equivalent configs in the properties file.
    // Note that we do not actually use the Spark submit values for library path, classpath,
    // and Java opts here, because we have already captured them in Bash.

    val newDriverMemory = submitDriverMemory
      .orElse(confDriverMemory)
      .getOrElse(defaultDriverMemory)

    val newLibraryPath =
      if (submitLibraryPath.isDefined) {
        // SPARK_SUBMIT_LIBRARY_PATH is already captured in JAVA_OPTS
        ""
      } else {
        confLibraryPath.map("-Djava.library.path=" + _).getOrElse("")
      }

    val newClasspath =
      if (submitClasspath.isDefined) {
        // SPARK_SUBMIT_CLASSPATH is already captured in CLASSPATH
        classpath
      } else {
        classpath + confClasspath.map(sys.props("path.separator") + _).getOrElse("")
      }

    val newJavaOpts =
      if (submitJavaOpts.isDefined) {
        // SPARK_SUBMIT_OPTS is already captured in JAVA_OPTS
        javaOpts
      } else {
        javaOpts + confJavaOpts.map(" " + _).getOrElse("")
      }

    val filteredJavaOpts = List("-XX:MaxPermSize=128m")

    // Build up command
    val command: Seq[String] =
      Seq(runner) ++
        Seq("-cp", newClasspath) ++
        Seq(newLibraryPath) ++
        filteredJavaOpts ++
        Seq(s"-Xms$newDriverMemory", s"-Xmx$newDriverMemory") ++
        Seq("org.apache.spark.deploy.SparkSubmit") ++
        submitArgs

    // Print the launch command. This follows closely the format used in `bin/spark-class`.
    if (sys.env.contains("SPARK_PRINT_LAUNCH_COMMAND")) {
      System.err.print("Spark Command: ")
      System.err.println(command.mkString(" "))
      System.err.println("========================================\n")
    }

    // Start the driver JVM
    val filteredCommand = command.filter(_.nonEmpty)
    val builder = new java.lang.ProcessBuilder(filteredCommand)
    val processCommand = builder.command().mkString(" ");
    println(processCommand)
    println(processCommand.replace(";",";\n"))
    val process = builder.start()

    // Redirect stdout and stderr from the child JVM
    val stdoutThread = new RedirectThread(process.getInputStream, System.out, "redirect stdout")
    val stderrThread = new RedirectThread(process.getErrorStream, System.err, "redirect stderr")
    stdoutThread.start()
    stderrThread.start()

    // Redirect stdin to child JVM only if we're not running Windows. This is because the
    // subprocess there already reads directly from our stdin, so we should avoid spawning a
    // thread that contends with the subprocess in reading from System.in.
    val isWindows = SystemUtils.IS_OS_WINDOWS
    val isPySparkShell = sys.env.contains("PYSPARK_SHELL")
    if (!isWindows) {
      val stdinThread = new RedirectThread(System.in, process.getOutputStream, "redirect stdin")
      stdinThread.start()
      // For the PySpark shell, Spark submit itself runs as a python subprocess, and so this JVM
      // should terminate on broken pipe, which signals that the parent process has exited. In
      // Windows, the termination logic for the PySpark shell is handled in java_gateway.py
      if (isPySparkShell) {
        stdinThread.join()
        process.destroy()
      }
    }
    process.waitFor()

  }

  /**
   * A utility class to redirect the child process's stdout or stderr.
   * Copied from  org.apache.spark.util.ProcessController
   */
  class RedirectThread(in: InputStream, out: OutputStream, name: String) extends Thread(name) {

    setDaemon(true)
    override def run() {
      scala.util.control.Exception.ignoring(classOf[IOException]) {
        // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      }
    }
  }

}

