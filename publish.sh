#!/bin/zsh
set -ex
if [ $(git diff | wc -l) != 0 ]
then
	echo "Commit your change before publish!"
	exit 1
fi

if [ -z "$1" ]
then
	PART_ID=0
else
	PART_ID=$1
fi
ROOT=$(dirname $(readlink -f $0))
OLD_VERSION=$(cat latest_version)
NEW_VERSION=$(awk -F. 'BEGIN{idx=3-'${PART_ID}'}{$idx+=1; for(i=idx+1; i <= 3; i++) $i=0; print $1"."$2"."$3}' ${ROOT}/latest_version)
echo "Publishing new version ${OLD_VERSION} -> ${NEW_VERSION}"

PATTERN=$(echo ^version = \"${OLD_VERSION}\"\$ | sed 's/\./\\./g')

sed -i "s/${PATTERN}/version = \"${NEW_VERSION}\"/g" **/Cargo.toml
echo ${NEW_VERSION} > latest_version

git add -u .
git commit -m "Bump version number from ${OLD_VERSION} to ${NEW_VERSION}"
git tag -a "v${NEW_VERSION}" -m "D4 ${NEW_VERSION} release"

git checkout -b "release-v${NEW_VERSION}"

sed -i 's/path[ ]*=[ ]*"..\/d4[^"]*"/version = "'${NEW_VERSION}'"/g' */Cargo.toml
git commit -am 'Update the dependency'

function publish-crate() {
	mv Cargo.toml Cargo.toml.tmp
	pushd $1
	cargo update
	cargo publish --registry crates-io
	popd
	mv Cargo.toml.tmp Cargo.toml
	sleep 20
}

publish-crate d4-hts
publish-crate d4-bigwig
publish-crate d4-framefile
publish-crate d4
publish-crate d4tools

git checkout master
git push origin
git push origin "v${NEW_VERSION}"
