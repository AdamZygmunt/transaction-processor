# Użyj oficjalnego obrazu Javy jako bazowego
FROM eclipse-temurin:17-jdk-alpine

# Ustaw katalog roboczy
WORKDIR /app

# Skopiuj plik JAR do obrazu
COPY build/libs/transaction-processor-0.0.1-SNAPSHOT.jar app.jar

# Uruchom aplikację
#ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED -verbose:class", "-jar", "app.jar"]
#ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED", "-jar", "app.jar"]
ENTRYPOINT ["java", "-Dspark.kryo.registrationRequired=false", "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer", "--add-opens=java.base/sun.security.action=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-jar", "app.jar"]
