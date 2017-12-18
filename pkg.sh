#!/usr/bin/env bash
output=build/pkg
rm -rf $output
mkdir -p $output

#
./gradlew assemble
cp build/libs/access-log-analyzer-*.jar $output
cp -rf data $output
cp -f ./script/run.sh $output

#
cd ./IPLoc/
./pkg.sh $1
cd ../
cp ./IPLoc/build/ips.zip $output

#
zip -r $output/access-log-analyzer.zip $output/*.sh $output/*.jar $output/*.zip

echo "All done..."
