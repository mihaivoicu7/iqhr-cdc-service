#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p target/maintenance
java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"
javac_bin="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
mode="${1:---compile-only}"
if [[ ! -f target/classes/com/iqhr/cdc/store/TenantStore.class ]]; then
  mvn -q -DskipTests compile
fi
scope=runtime
[[ "$mode" == "--self-test" ]] && scope=test
classpath_file="target/maintenance/$scope-classpath.txt"
if [[ ! -f "$classpath_file" || pom.xml -nt "$classpath_file" ]]; then
  mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    -DincludeScope="$scope" -Dmdep.outputFile="$classpath_file"
fi
classpath="target/classes:$(cat "$classpath_file")"
"$javac_bin" --release 21 -cp "$classpath" -d target/maintenance scripts/maintenance/CleanupTestProbes.java
if [[ "$mode" == "--compile-only" ]]; then
  printf '%s\n' 'Standalone JPA cleanup operator compiled; no database connection opened.'
elif [[ "$mode" == "--self-test" ]]; then
  "$javac_bin" --release 21 -cp "target/maintenance:$classpath" -d target/maintenance scripts/maintenance/CleanupTestProbesSelfTest.java
  "$java_bin" -Dlogback.configurationFile=scripts/maintenance/logback.xml -cp "target/maintenance:$classpath" com.iqhr.cdc.maintenance.CleanupTestProbesSelfTest
elif [[ "$mode" == "--apply" ]]; then
  "$java_bin" -Dlogback.configurationFile=scripts/maintenance/logback.xml -cp "target/maintenance:$classpath" com.iqhr.cdc.maintenance.CleanupTestProbes --apply
else
  printf '%s\n' 'Use --compile-only, --self-test or --apply.' >&2
  exit 2
fi
