#!/bin/bash
./gradlew build
java -jar NATT.jar -nc https://gitlab.com/ap5vs/test-config/mqtt-client-config/-/raw/main/test-config-8.yaml?ref_type=heads
