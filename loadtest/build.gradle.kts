plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
    application
}

application {
    mainClass.set("loadtest.MainKt")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")
    implementation("ch.qos.logback:logback-classic:1.4.14")
}

kotlin {
    jvmToolchain(17)
}
