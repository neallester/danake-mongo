swift package clean
rm -rf .build
swift package update
swift package generate-xcodeproj
