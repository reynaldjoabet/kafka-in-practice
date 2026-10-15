// The Play plugin
addSbtPlugin("org.playframework" % "sbt-plugin" % "3.1.0-M9")

addSbtPlugin("com.jamesward" % "sbt-mcp"      % "0.1.5")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
// Newer than the 1.11.7 the Play plugin brings.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.12.0")
// Version and git commit compiled into the app (com.lattice.oidc.BuildInfo).
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo" % "0.13.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"       % "0.4.8")

addSbtPlugin("com.github.sbt" % "sbt-digest" % "2.2.0-M1")

addSbtPlugin("com.github.sbt" % "sbt-gzip" % "2.1.0-M1")
