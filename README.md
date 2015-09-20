## Spark Testing Example Application

Source code for blog posts:

- [Running Spark tests in standalone cluster](http://eugenezhulenev.com/blog/2014/10/18/run-tests-in-standalone-spark-cluster/)
http://eugenezhulenev.com/blog/2014/10/18/run-tests-in-standalone-spark-cluster/

## Testing

By default tests are running with Spark in local mode

    sbt test

If you want to run tests in standalone Spark cluster you need to provide `spark.master` url

    sbt -Dspark.master=spark://spark-host:7777 test-assembled

## Building

In the root directory run:

    sbt assembly

The application fat jars will be placed in:
  - `target/scala-2.10/spark-testing-example-app.jar`


## Running

First you need to run `assembly` in sbt and then run java cmd

    java -Dspark.master=spark://spark-host:7777 spark-testing-example-app.jar
    
---

Run '1442575127032' of group 'propgroup' failed to execute 'DataComparArator'.\n
Exception Type: ClassNotFoundException Message: kanakku.entrySyscesium.DataComparArator$anonfun$10$anonfun$apply$1 StackTrace:
 java.net.URLClassLoader$1.run(URLClassLoader.java:366)
 	java.net.URLClassLoader$1.run(URLClassLoader.java:355)
 java.security.AccessController.doPrivileged(Native Method)
 java.net.URLClassLoader.findClass(URLClassLoader.java:354)
 java.lang.ClassLoader.loadClass(ClassLoader.java:425)
 sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:308)
 java.lang.ClassLoader.loadClass(ClassLoader.java:358
 java.lang.Class.forName0(Native Method)
 java.lang.Class.forName(Class.java:270)
 org.apache.spark.util.InnerClosureFinder$anon$4.visitMethodInsn(ClosureCleaner.scala:260)
 com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassReader.accept(Unknown Source) 
 com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassReader.accept(Unknown Source) org.apache.spark.util.ClosureCleaner$.getInnerClasses(ClosureCleaner.scala:87)
 org.apache.spark.util.ClosureCleaner$.clean(ClosureCleaner.scala:107)
 org.apache.spark.SparkContext.clean(SparkContext.scala:1440)
 org.apache.spark.rdd.RDD.map(RDD.scala:271)
 kanakku.entrySyscesium.DataComparArator.execute(DataComparArator.scala:212)
 porp.sparkserver.actors.SparkJob.porp$sparkserver$actors$SparkJob$runRule(SparkJob.scala:140)
 porp.sparkserver.actors.SparkJob$anonfun$porp$sparkserver$actors$SparkJob$startJob$1.runRules$1(SparkJob.scala:101)
 porp.sparkserver.actors.SparkJob$anonfun$porp$sparkserver$actors$SparkJob$startJob$1.apply$mcV$sp(SparkJob.scala:113)
 porp.sparkserver.actors.SparkJob$anonfun$porp$sparkserver$actors$SparkJob$startJob$1.apply(SparkJob.scala:93)
 porp.sparkserver.actors.SparkJob$anonfun$porp$sparkserver$actors$SparkJob$startJob$1.apply(SparkJob.scala:93)
 scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
 scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
 akka.dispatch.TaskInvocation.run(AbstractDispatcher.scala:42)
 java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
 java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
 java.lang.Thread.run(Thread.java:744)
 `
